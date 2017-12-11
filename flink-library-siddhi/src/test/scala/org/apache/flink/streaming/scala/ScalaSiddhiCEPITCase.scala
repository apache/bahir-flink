/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala

import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.siddhi.ScalaSiddhiCEP._
import org.apache.flink.streaming.siddhi.SiddhiCEP
import org.apache.flink.streaming.siddhi.extension.CustomPlusFunctionExtension
import org.apache.flink.streaming.siddhi.source.{Event, RandomEventSource}
import org.junit.rules.TemporaryFolder
import org.junit.{Rule, Test}

class ScalaSiddhiCEPITCase extends ScalaStreamingMultipleProgramsTestBase  {
  val _tempFolder = new TemporaryFolder()

  @Rule
  def tempFolder: TemporaryFolder = _tempFolder

  @Test
  def testJavaApiInScala(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val input = env.fromElements(
      Event.of(1, "start", 1.0),
      Event.of(2, "middle", 2.0),
      Event.of(3, "end", 3.0),
      Event.of(4, "start", 4.0),
      Event.of(5, "middle", 5.0),
      Event.of(6, "end", 6.0)
    )

    val output = SiddhiCEP
      .define("inputStream", input, "id", "name", "price")
      .cql("from inputStream insert into  outputStream")
      .returns("outputStream", classOf[Event])
    val path = tempFolder.newFile.toURI.toString
    output.print
    env.execute
  }

  @Test
  def testSiddhiCEPImplicits(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    implicit val siddhiCEP = env.getSiddhiEnvironment()
    env.fromElements(
        Event.of(1, "start", 1.0),
        Event.of(2, "middle", 2.0),
        Event.of(3, "end", 3.0),
        Event.of(4, "start", 4.0),
        Event.of(5, "middle", 5.0),
        Event.of(6, "end", 6.0))
      .schema("inputStream", Seq("id", "name", "price"))
      .cql("from inputStream insert into  outputStream")
      .returns("outputStream", classOf[Event])
      .print
    env.execute
  }

  @Test
  def testRegisterStreamAndExtensionWithSiddhiCEPEnvironment(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    implicit val siddhi = env.getSiddhiEnvironment()

    extend("custom:plus", classOf[CustomPlusFunctionExtension])

    env.addSource(new RandomEventSource(5), "input1").keyBy("id")
      .schema("inputStream1", Seq("id", "name", "price", "timestamp"))
    env.addSource(new RandomEventSource(5), "input2").keyBy("id")
      .schema("inputStream2", Seq("id", "name", "price", "timestamp"))

    from("inputStream1").union("inputStream2")
      .cql("from inputStream1#window.length(5) as s1 "
        + "join inputStream2#window.time(500) as s2 "
        + "on s1.id == s2.id "
        + "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
        + "insert into JoinStream;").returns("JoinStream")
      .writeAsText(tempFolder.newFile.toURI.toString, FileSystem.WriteMode.OVERWRITE)

    env.execute
  }
}