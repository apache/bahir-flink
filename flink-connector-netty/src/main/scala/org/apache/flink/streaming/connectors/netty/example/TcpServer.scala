/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.netty.example

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{ChannelFuture, ChannelInitializer, ChannelOption, ChannelPipeline}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

/**
 * Netty Server bootstrap with user-provided tcp port.
 * - Receiving streaming data
 * - add to Flink [[org.apache.flink.hadoop.shaded.org.jboss.netty.channel.ChannelHandlerContext]]
 *
 * @param tryPort     port start to retry
 * @param ctx         flink stream collect data from netty
 * @param tcpOpts     tcp option for netty server
 * @param threadNum   thread number for netty, default is current machine processor number
 * @param maxFrameLen max netty frame length
 * @param logLevel    netty log level
 */
class TcpServer(
  tryPort: Int,
  ctx: SourceContext[String],
  tcpOpts: ServerBootstrap => Unit,
  threadNum: Int = Runtime.getRuntime.availableProcessors(),
  maxFrameLen: Int = 8192,
  logLevel: LogLevel = LogLevel.INFO
) extends ServerTrait {

  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val bossGroup = new NioEventLoopGroup(threadNum)
  private lazy val workerGroup = new NioEventLoopGroup
  private lazy val isRunning = new AtomicBoolean(false)

  private var currentAddr: InetSocketAddress = _

  def startNettyServer(
    portNotInUse: Int,
    callbackUrl: Option[String]
  ): InetSocketAddress = synchronized {
    if (!isRunning.get()) {

      val server = new ServerBootstrap
      val bootstrap: ServerBootstrap = server
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
        .option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)

      tcpOpts(bootstrap)

      val bootWithHandler = bootstrap
        .handler(new LoggingHandler(logLevel))
        .childHandler(new ChannelInitializer[SocketChannel]() {
          def initChannel(ch: SocketChannel) {
            val p: ChannelPipeline = ch.pipeline
            p.addLast(new DelimiterBasedFrameDecoder(maxFrameLen, Delimiters.lineDelimiter(): _*))
            p.addLast(new StringEncoder())
            p.addLast(new StringDecoder())
            p.addLast(new TcpHandler(ctx))
          }
        })

      // Start the server.
      val f: ChannelFuture = bootWithHandler.bind(portNotInUse)
      f.syncUninterruptibly()
      currentAddr = f.channel().localAddress().asInstanceOf[InetSocketAddress]
      logger.info(s"start tcp server on address: $currentAddr")
      isRunning.set(true)
      register(currentAddr, callbackUrl)
      f.channel().closeFuture().sync()
      currentAddr
    } else {
      logger.info(s"server is running on address: $currentAddr, no need repeat start it")
      currentAddr
    }
  }

  override def close(): Unit = {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    logger.info("successfully close netty server source")
  }
}

object TcpServer {

  def apply(
    tryPort: Int,
    ctx: SourceContext[String],
    threadNum: Int = Runtime.getRuntime.availableProcessors(),
    maxFrameLen: Int = 8192,
    logLevel: LogLevel = LogLevel.INFO
  ): TcpServer = {
    val tcpOptions = (bootstrap: ServerBootstrap) => {}
    new TcpServer(tryPort, ctx, tcpOptions, threadNum, maxFrameLen, logLevel)
  }
}