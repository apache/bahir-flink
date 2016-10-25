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

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

/**
 * A end-to-end source, build a keep-alive tcp channel by netty.
 *
 * When this source stream get start, listen a provided tcp port, receive stream data sent from
 * the place where origin data generated.
 * {{{
 *   // for example:
 *   val env = StreamExecutionEnvironment.getExecutionEnvironment
 *   env.addSource(new TcpReceiverSource("msg", 7070, Some("http://localhost:9090/cb")))
 * }}}
 * The features provide by this source:
 * 1. source run as a netty tcp server
 * 2. listen provided tcp port, if the port is in used,
 * increase the port number between 1024 to 65535
 * 3. callback the provided url to report the real port to listen
 *
 * @param tryPort     the tcp port to start, if port Collision, retry a new port
 * @param callbackUrl when netty server started, report the ip and port to this url
 */
final class TcpReceiverSource(
  tryPort: Int,
  callbackUrl: Option[String] = None
) extends RichParallelSourceFunction[String] {
  private var server: TcpServer = _

  override def cancel(): Unit = server.close()

  override def run(ctx: SourceContext[String]): Unit = {
    server = TcpServer(tryPort, ctx)
    server.start(tryPort, callbackUrl)
  }
}