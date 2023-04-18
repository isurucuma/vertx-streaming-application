package com.example.vertx.streaming;

import com.example.vertx.streaming.jukebox.Jukebox;
import com.example.vertx.streaming.jukebox.NetControl;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class Main extends AbstractVerticle {

  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(new Main());
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    vertx.deployVerticle(new Jukebox());
    vertx.deployVerticle(new NetControl());
  }
}
