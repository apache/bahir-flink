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

package org.apache.flink.streaming.connectors.pinot.segment.name;

import org.apache.pinot.core.segment.name.SegmentNameGenerator;

import java.io.Serializable;

/**
 * Defines the segment name generator interface that is used to generate segment names. The segment
 * name generator is required to be serializable. We expect users to inherit from
 * {@link PinotSinkSegmentNameGenerator} in case they want to define their custom name generator.
 */
public interface PinotSinkSegmentNameGenerator extends SegmentNameGenerator, Serializable {
}
