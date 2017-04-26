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

import java.util
import java.util.concurrent.{Executors, LinkedBlockingQueue, TimeUnit}

import com.alibaba.fastjson.JSONObject
import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.impl.client.HttpClients
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.slf4j.{Logger, LoggerFactory}

/** base test util */
class BaseTest {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  private lazy val httpclient = HttpClients.createDefault()
  private lazy val schedule = Executors.newScheduledThreadPool(20)
  private lazy val pool = Executors.newCachedThreadPool()


  def schedule(period: Int, f: () => Unit): Unit = {
    schedule.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        f.apply()
      }
    }, 3, period, TimeUnit.SECONDS)
  }

  def run(f: () => Unit): Unit = {
    pool.submit(new Runnable {
      override def run(): Unit = {
        f.apply()
      }
    })
  }

  def sendGetRequest(url: String): String = {
    val httpGet = new HttpGet(url)
    val response1 = httpclient.execute(httpGet)
    try {
      logger.info(s"response: ${response1.getStatusLine}, url:$url")
      val entity = response1.getEntity
      EntityUtils.toString(entity)
    } finally {
      response1.close()
    }
  }

  def sendPostRequest(url: String, map: Map[String, String]): String = {
    val httpPost = new HttpPost(url)
    val nvps = new util.ArrayList[NameValuePair]()
    map.foreach { kv =>
      nvps.add(new BasicNameValuePair(kv._1, kv._2))
    }
    httpPost.setEntity(new UrlEncodedFormEntity(nvps))
    val response = httpclient.execute(httpPost)
    try {
      logger.info("response status line:" + response.getStatusLine)
      val entity2 = response.getEntity
      EntityUtils.toString(entity2)
    } finally {
      response.close()
    }
  }
}
