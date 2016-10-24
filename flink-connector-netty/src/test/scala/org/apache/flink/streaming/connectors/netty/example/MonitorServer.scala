/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.netty.example

import java.net.URLDecoder
import java.util.concurrent.LinkedBlockingQueue

import com.alibaba.fastjson.JSONObject
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.Unpooled
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.http._
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.slf4j.LoggerFactory

class MonitorServer(queue: LinkedBlockingQueue[JSONObject]) {
  def start(port: Int): Unit = {
    // Configure the server.
    val bossGroup: EventLoopGroup = new NioEventLoopGroup(1)
    val workerGroup: EventLoopGroup = new NioEventLoopGroup
    val b: ServerBootstrap = new ServerBootstrap
    b.option[java.lang.Integer](ChannelOption.SO_BACKLOG, 1024)
    b
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])
      .handler(new LoggingHandler(LogLevel.INFO))
      .childHandler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          val p = ch.pipeline()
          p.addLast(new HttpServerCodec)
          p.addLast(new MonitorHandler(queue))
        }
      })
    val f = b.bind(port).syncUninterruptibly()
    //    println("Open your web browser and navigate to http://127.0.0.1:" + port + '/')
    f.channel().closeFuture().sync()
  }
}


class MonitorHandler(list: LinkedBlockingQueue[JSONObject]) extends ChannelInboundHandlerAdapter {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = ctx.flush

  private def parseCallback(line: String): JSONObject = {
    val obj = new JSONObject()
    line.startsWith("/cb?") match {
      case true =>
        val map = line.substring(4).split("&")
        map.foreach(f => {
          val tp = f.split("=")
          obj.put(tp.head, URLDecoder.decode(tp(1), "UTF-8"))
        })
      case false =>
        logger.info("")
    }
    obj
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = {
    msg match {
      case req: HttpRequest =>
        val uri = req.uri()
        //  add to queue
        if (uri != "/favicon.ico") {
          list.put(parseCallback(uri))
        }
        logger.info("received data: " + uri)

        val ack: Array[Byte] = Array('O', 'K')
        val response: FullHttpResponse = new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpResponseStatus.OK,
          Unpooled.wrappedBuffer(ack)
        )
        response.headers.set("Content-Type", "text/plain")
        response.headers.set("Content-Length", response.content.readableBytes)
        ctx.write(response).addListener(ChannelFutureListener.CLOSE)
      case _ =>
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close
  }
}
