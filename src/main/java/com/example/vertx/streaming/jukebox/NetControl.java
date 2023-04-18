package com.example.vertx.streaming.jukebox;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;

public class NetControl extends AbstractVerticle {
  public void start(Promise<Void> startVerticle){
    vertx.createNetServer()
      .connectHandler(this::handleClient)
      .listen(3000);
  }

  private void handleClient(NetSocket socket){
    RecordParser.newDelimited("\n", socket)
      .handler(buffer -> handleBuffer(socket, buffer))
      .endHandler(v -> System.out.println("Connection ended..."));
  }

  private void handleBuffer(NetSocket socket, Buffer buffer) {
    String command = buffer.toString();
    switch (command){
      case "/list":
        listCommand(socket);
        break;

      case "/play":
        vertx.eventBus().send("jukebox.play", "");
        break;

      case "/pause":
        vertx.eventBus().send("jukebox.pause", "");
        break;
      default:
        if(command.startsWith("/schedule ")){
          schedule(command);
        }else{
          socket.write("Unknown command\n");
        }
    }

  }

  private void schedule(String command){
    String track = command.substring(10);
    JsonObject json = new JsonObject().put("file", track);
    vertx.eventBus().send("jukebox.schedule", json);
  }

  private void listCommand(NetSocket socket) {
    vertx.eventBus().request("jukebox.list", "", reply -> {
      if (reply.succeeded()) {
        JsonObject data = (JsonObject) reply.result().body();
        data.getJsonArray("files")
          .stream().forEach(name -> socket.write(name + "\n"));
      } else {
        System.err.println("/list_error" + reply.cause());
      }
    });
  }

}
