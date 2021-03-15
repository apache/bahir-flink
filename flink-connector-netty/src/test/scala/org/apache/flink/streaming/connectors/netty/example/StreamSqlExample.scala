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

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * Simple example for demonstrating the use of SQL on a Stream Table.
 *
 * This example shows how to:
 *  - Convert DataStreams to Tables
 *  - Register a Table under a name
 *  - Run a StreamSQL query on the registered Table
 */
object StreamSqlExample {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  def main(args: Array[String]): Unit = {
    val param = ParameterTool.fromArgs(args)

    // set up execution environment
    val envSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env, envSettings)

    val spec = if (param.get("tcp") == "true") {
      new TcpReceiverSource(7070, Some("http://localhost:9090/cb"))
    } else {
      new HttpReceiverSource("msg", 7070, Some("http://localhost:9090/cb"))
    }

    val orderA: DataStream[Order] = env
      .addSource(spec)
      .setParallelism(3)
      .map { line =>
        val tk = line.split(",")
        Order(tk.head.trim.toLong, tk(1), tk(2).trim.toInt)
      }

    tEnv.createTemporaryView("OrderA", orderA)

    // union the two tables
    val result = tEnv.sqlQuery("SELECT * FROM OrderA WHERE amount > 2")

    result.toAppendStream[Order].print()

    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class Order(user: Long, product: String, amount: Int)

}
