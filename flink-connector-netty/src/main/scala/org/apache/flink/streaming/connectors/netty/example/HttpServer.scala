package org.apache.flink.streaming.connectors.netty.example

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.channel.{Channel, ChannelInitializer, ChannelOption}
import io.netty.handler.codec.http.HttpServerCodec
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

/**
  * netty http server
  * Created by shijinkui on 9/24/16.
  */
class HttpServer(
  ctx: SourceContext[String],
  tryPort: Int,
  threadNum: Int = Runtime.getRuntime.availableProcessors(),
  logLevel: LogLevel = LogLevel.INFO
) extends ServerTrait {
  private lazy val logger = LoggerFactory.getLogger(getClass)
  private lazy val bossGroup = new NioEventLoopGroup(threadNum)
  private lazy val workerGroup = new NioEventLoopGroup
  private lazy val isRunning = new AtomicBoolean(false)

  private var currentAddr: InetSocketAddress = _

  override def close(): Unit = {
    bossGroup.shutdownGracefully()
    workerGroup.shutdownGracefully()
    logger.info("successfully close netty server source")
  }

  def startNettyServer(
    portNotInUse: Int,
    callbackUrl: Option[String]
  ): InetSocketAddress = synchronized {

    if (!isRunning.get()) {
      val b: ServerBootstrap = new ServerBootstrap
      b
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 1024)
        .group(bossGroup, workerGroup)
        .channel(classOf[NioServerSocketChannel])
        .handler(new LoggingHandler(logLevel))
        .childHandler(new ChannelInitializer[SocketChannel] {
          override def initChannel(ch: SocketChannel): Unit = {
            val p = ch.pipeline()
            p.addLast(new HttpServerCodec)
            p.addLast(new HttpHandler(ctx))
          }
        })
      val f = b.bind(portNotInUse)
      f.syncUninterruptibly()
      val ch: Channel = f.channel()
      isRunning.set(true)
      currentAddr = ch.localAddress().asInstanceOf[InetSocketAddress]
      register(currentAddr, callbackUrl)
      ch.closeFuture().sync()
      currentAddr
    } else {
      currentAddr
    }
  }
}
