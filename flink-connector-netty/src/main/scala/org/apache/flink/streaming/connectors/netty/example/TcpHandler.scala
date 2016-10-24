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

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.slf4j.LoggerFactory

/**
 * process netty stream data, add to flink
 */
private class TcpHandler(sctx: SourceContext[String]) extends SimpleChannelInboundHandler[String] {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  override def channelRead0(ctx: ChannelHandlerContext, msg: String): Unit = {
    sctx.collect(msg)
  }

  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    logger.info(s"tcp channel active, remote address:${ctx.channel().remoteAddress()}")
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = ctx.flush

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
    logger.error(s"netty server channel ${ctx.channel()} error", cause)
    ctx.close()
  }
}
