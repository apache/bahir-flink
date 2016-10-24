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

import io.netty.bootstrap.Bootstrap
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.slf4j.LoggerFactory

private class NettyClient(host: String, port: Int) extends Thread {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private lazy val group: EventLoopGroup = new NioEventLoopGroup
  private var ch: Channel = _

  def shutdown(): Unit = {
    group.shutdownGracefully()
  }

  def send(line: String): Unit = {
    if (ch.isActive && ch != null) {
      ch.writeAndFlush(line + "\n")
      logger.info("client send msg: "
        + s"${ch.isActive} ${ch.isOpen}  ${ch.isRegistered} ${ch.isWritable}")
    } else {
      logger.info("client fail send msg, "
        + s"${ch.isActive} ${ch.isOpen}  ${ch.isRegistered} ${ch.isWritable}")
    }
  }

  override def run(): Unit = {

    val b: Bootstrap = new Bootstrap
    b.group(group)
      .channel(classOf[NioSocketChannel])
      .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
      .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      .handler(new ChannelInitializer[SocketChannel]() {
        def initChannel(ch: SocketChannel) {
          val p: ChannelPipeline = ch.pipeline
          p.addLast(new LoggingHandler(LogLevel.INFO))
          p.addLast(new DelimiterBasedFrameDecoder(8192, Delimiters.lineDelimiter(): _*))
          p.addLast(new StringEncoder())
          p.addLast(new StringDecoder())
          p.addLast(new NettyClientHandler)
        }
      })
    // Start the client.
    val f: ChannelFuture = b.connect(host, port).sync()
    ch = f.channel()
  }
}

