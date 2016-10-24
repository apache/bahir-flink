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

import java.io.Closeable
import java.net.{InetSocketAddress, URLEncoder}

/**
 * server trait
 */
trait ServerTrait extends Closeable {

  def start(tryPort: Int, callbackUrl: Option[String]): InetSocketAddress = {
    val addr = NettyUtil.startServiceOnPort(tryPort, (p: Int) => startNettyServer(p, callbackUrl))
    //  register to monitor system
    //    register(addr, callbackUrl)
    addr
  }

  def startNettyServer(portNotInUse: Int, callbackUrl: Option[String]): InetSocketAddress

  def register(address: InetSocketAddress, callbackUrl: Option[String]): Unit = {
    callbackUrl match {
      case Some(url) =>
        val ip = address.getAddress.getHostAddress
        val newIp = if (ip.startsWith("0") || ip.startsWith("127")) {
          NettyUtil.findLocalInetAddress().getHostAddress
        } else {
          ip
        }
        val port = address.getPort
        val param = s"ip=${URLEncoder.encode(newIp, "UTF-8")}&port=$port"
        val callUrl = if (url.endsWith("?")) param else "?" + param
        println("register callback url:" + callUrl)
        NettyUtil.sendGetRequest(url + callUrl)
      case _ =>
    }
  }
}
