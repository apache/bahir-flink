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

import io.netty.channel._
import org.slf4j.LoggerFactory

import scala.util.Random


final class NettyClientHandler extends SimpleChannelInboundHandler[String] {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    val ch = ctx.channel()
    logger.info(s"active channel: $ch")
  }

  override def channelInactive(ctx: ChannelHandlerContext) {
    val ch = ctx.channel()
    logger.info(s"inactive channel: $ch")
  }

  override def channelRead0(ctx: ChannelHandlerContext, msg: String) {
    logger.info("receive message:" + msg)
    ctx.writeAndFlush(Random.nextLong() + ",sjk," + Random.nextInt())
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = ctx.flush

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    cause.printStackTrace()
    ctx.close
  }
}
