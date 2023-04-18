package com.example.vertx.streaming.jukebox;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


public class Jukebox extends AbstractVerticle {

  private enum State {PAUSED, PLAYING}
  private State currentState = State.PAUSED;
  private final Queue<String> playlist = new ArrayDeque<>();
  private final Set<HttpServerResponse> streamers = new HashSet<>();
  private AsyncFile currentFile;
  private long positionInFile;

  public void start(Promise<Void> startPromise){
    EventBus eventBus = vertx.eventBus();
    eventBus.consumer("jukebox.list", this::list);
    eventBus.consumer("jukebox.schedule", this::schedule);
    eventBus.consumer("jukebox.play", this::play);
    eventBus.consumer("jukebox.pause", this::pause);

    vertx.createHttpServer()
      .requestHandler(this::httpHandler)
      .listen(8080);

    vertx.setPeriodic(100, this::streamAudioChunk);
  }

  private void streamAudioChunk(Long id) {
    if(currentState == State.PAUSED){
      return;
    }
    if(currentFile == null && playlist.isEmpty()){ // if all the files finished stream
      currentState = State.PAUSED;
      return;
    }
    if(currentFile == null){ //  current file is finished and there are other files in the queue to deliver
      openNextFile();
    }
    // buffer initial size, offest in the buffer to start writing, position in the dile, length if the buffer to read
    currentFile.read(Buffer.buffer(4096), 0, positionInFile, 4096, ar -> {
      if(ar.succeeded()){
        processReadBuffer(ar.result());
      }else{
        System.err.println("Read Failed...");
        closeCurrentFile();
      }
    });

  }

  private void processReadBuffer(Buffer buffer){
    positionInFile += buffer.length();
    if(buffer.length() == 0){ //  this happend at the end of the file
      closeCurrentFile();
      return;
    }
    for(HttpServerResponse streamer : streamers){
      if(!streamer.writeQueueFull()){ // back pressure handling
        streamer.write(buffer.copy()); // because buffers cannot be reused, this is the best way to deal with this. Don't look for unnecessary optimizations here
      }
    }
  }

  private void openNextFile(){
    OpenOptions opts = new OpenOptions().setRead(true);
    currentFile = vertx.fileSystem()
      .openBlocking("tracks/" + playlist.poll(), opts); // here we are using blocking, but since this files are in the same server this wil not be an issue
    positionInFile = 0;
  }

  private void closeCurrentFile(){
    positionInFile = 0;
    currentFile.close();
    currentFile = null;
  }

  private void httpHandler(HttpServerRequest request){
    if("/".equals(request.path())){
      openAudioStream(request);
      return;
    }
    if(request.path().startsWith("/download/")){
      String sanitizedPath = request.path().substring(10).replaceAll("/", "");
      download(sanitizedPath, request);
      return;
    }
    request.response().setStatusCode(404).end();
  }

  private void play(Message<?> request){
    currentState = State.PLAYING;
  }
  private void pause(Message<?> request){
    currentState = State.PAUSED;
  }

  private void schedule(Message<JsonObject> request){
    String file = request.body().getString("file");
    if(playlist.isEmpty() && currentState == State.PAUSED){
      currentState = State.PLAYING;
    }
    playlist.offer(file);
  }

  private void list(Message<?> request){
    String path = Paths.get(".").toAbsolutePath().normalize() + "/tracks";
    vertx.fileSystem().readDir(path, ".*mp3$", ar -> {
      if(ar.succeeded()){
        List<String> files = ar.result()
          .stream()
          .map(File::new)
          .map(File::getName)
          .collect(Collectors.toList());
        JsonObject json = new JsonObject().put("files", new JsonArray(files));
        request.reply(json);
      }
      else{
        System.err.println("read dir failed" + ar.cause().getMessage());
        request.fail(500, ar.cause().getMessage());
      }
    });
  }
  private void openAudioStream(HttpServerRequest request){
    HttpServerResponse response = request.response()
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true);
    streamers.add(response);
    response.endHandler(v -> {
      streamers.remove(response);
      System.out.println("A streamer left...");
    });
  }

  private void download(String path, HttpServerRequest request){
    String file = "tracks/" + path;
    if(!vertx.fileSystem().existsBlocking(file)){
      request.response().setStatusCode(404).end();
      return;
    }
    OpenOptions opts = new OpenOptions().setRead(true);
    vertx.fileSystem().open(file, opts, ar -> {
      if(ar.succeeded()){
        downloadFile(ar.result(), request);
      }else{
        System.err.println("Read failed...");
        request.response().setStatusCode(500).end();
      }
    });
  }

  private void downloadFile(AsyncFile file, HttpServerRequest request){
    HttpServerResponse response = request.response();
    response.setStatusCode(200)
      .putHeader("Content-Type", "audio/mpeg")
      .setChunked(true);

    /*
    file.handler(buffer -> {
      response.write(buffer);
      if(response.writeQueueFull()){ // check the back pressure
        file.pause(); // pausing the file read
        response.drainHandler(v -> file.resume()); // resume the file read whcn the response buffer is half
      }
    });
    file.endHandler(v -> response.end()); // when there is no more data to be read in the file, this handler is called
    */

    file.pipeTo(response); // This can be replaced with the above back pressure handling code segment A pipe deals with back-pressure when copying between a passable ReadStream and a WriteStream.
  }
}
