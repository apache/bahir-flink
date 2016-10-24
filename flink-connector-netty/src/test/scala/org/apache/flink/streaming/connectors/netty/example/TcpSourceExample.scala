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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.alibaba.fastjson.JSONObject

import scala.util.Random

object TcpSourceExample extends BaseTest {

  def main(args: Array[String]): Unit = {
    val queue = new LinkedBlockingQueue[JSONObject]()

    //  1.  register server, wait for flink netty source server started
    run(() => new MonitorServer(queue).start(9090))
    //  2.  start flink job
    run(() => StreamSqlExample.main(Array("--tcp", "true")))

    Thread.sleep(5000)

    //  3.  sending message to netty source continuously
    while (true) {
      logger.info("==============")
      val json = queue.poll(Int.MaxValue, TimeUnit.SECONDS)
      logger.info("====request register from netty tcp source: " + json)
      val client = new NettyClient("localhost", json.getInteger("port"))
      client.run()
      schedule(5, () => {
        val line = Random.nextLong() + ",sjk," + Random.nextInt()
        client.send(line)
      })
    }
  }
}
