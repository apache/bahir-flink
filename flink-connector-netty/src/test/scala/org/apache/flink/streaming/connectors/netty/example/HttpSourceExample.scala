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

import java.net.URLEncoder
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import com.alibaba.fastjson.JSONObject

import scala.util.Random

/**
 * http client
 */
object HttpSourceExample extends BaseTest {

  def main(args: Array[String]): Unit = {
    val queue = new LinkedBlockingQueue[JSONObject]()

    run(() => new MonitorServer(queue).start(9090))
    run(() => StreamSqlExample.main(Array("--http", "true")))

    Thread.sleep(5000)

    while (true) {
      val json = queue.poll(Int.MaxValue, TimeUnit.SECONDS)
      logger.info("====request register from netty tcp source: " + json)
      val url = s"http://${json.getString("ip")}:${json.getString("port")}/payload?msg="
      schedule(5, () => {
        val line = s"${Random.nextInt(5)},abc,${Random.nextInt(100)}"
        sendGetRequest(url + URLEncoder.encode(line, "UTF-8"))
      })
    }
  }
}
