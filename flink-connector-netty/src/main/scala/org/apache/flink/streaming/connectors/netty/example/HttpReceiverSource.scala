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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * Http receiver source is used for receiving pushed http request.
 * It work for two step:
 * 1. start netty server with an un-used port when Flink get start
 * 2. after started netty, call back [[callbackUrl]] for register current connector to
 * message service, user can push http message to this address.
 * {{{
 *   // for example:
 *   val env = StreamExecutionEnvironment.getExecutionEnvironment
 *   env.addSource(new TcpReceiverSource(7070, Some("http://localhost:9090/cb")))
 * }}}
 *
 * @param paramKey    the http query param key
 * @param tryPort     try to use this point, if this point is used then try a new port
 * @param callbackUrl register connector's ip and port to a third service
 */
final class HttpReceiverSource(
  paramKey: String,
  tryPort: Int,
  callbackUrl: Option[String] = None
) extends RichParallelSourceFunction[String] {
  private var server: HttpServer = _

  override def cancel(): Unit = server.close()

  override def run(ctx: SourceContext[String]): Unit = {
    server = new HttpServer(ctx, paramKey)
    server.start(tryPort, callbackUrl)
  }
}