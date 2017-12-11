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

package org.apache.flink.streaming.siddhi

import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.siddhi.SiddhiStream.SingleSiddhiStream

import scala.collection.JavaConverters._

class ScalaSiddhiStream[T](val dataStream: DataStream[T]) {
  def schema(streamId: String, fieldNames: Seq[String])(implicit env: SiddhiCEP) :
  SiddhiStream.SingleSiddhiStream[T] = {
    env.registerStream(streamId, dataStream, fieldNames.asJava)
    env.from(streamId);
  }
}

class ScalaSiddhiEnvironment(val env: StreamExecutionEnvironment) {
  def getSiddhiEnvironment(): SiddhiCEP = SiddhiCEP.getSiddhiEnvironment(env)
}

object ScalaSiddhiCEP {
  implicit def getSiddhiStream[T](dataStream: DataStream[T]): ScalaSiddhiStream[T] =
    new ScalaSiddhiStream(dataStream)

  implicit def getSiddhiEvn[T](env: StreamExecutionEnvironment): ScalaSiddhiEnvironment =
    new ScalaSiddhiEnvironment(env)


  def from[T](streamId: String)(implicit env: SiddhiCEP): SingleSiddhiStream[T] = {
    env.from(streamId);
  }

  def extend(extensionName: String, extensionClass: Class[_])(implicit env: SiddhiCEP) :
  SiddhiCEP = {
    env.registerExtension(extensionName, extensionClass)
    env
  }
}