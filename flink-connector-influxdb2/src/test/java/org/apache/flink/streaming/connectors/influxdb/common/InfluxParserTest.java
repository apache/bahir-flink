/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.connectors.influxdb.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.text.ParseException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class InfluxParserTest {

    @Test
    void shouldParseLineWithTagAndFieldStringToPoint() throws ParseException {
        final String lineProtocol =
                "test,testTagKey=testTagValue testFieldKey=\"testFieldValue\" 1556813561098000000";
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        final DataPoint expectedDataPoint = new DataPoint("test", 1556813561098000000L);
        expectedDataPoint.addTag("testTagKey", "testTagValue");
        expectedDataPoint.addField("testFieldKey", "testFieldValue");

        assertEquals(expectedDataPoint, actualDataPoint);
    }

    @Test
    void shouldParseNotDuplicatedLineToDataPoint() throws ParseException {
        final String lineProtocol =
                "test,testTagKey=testTagValue testFieldKey=\"testFieldValue\" 1556813561098000000";
        final DataPoint dataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        final String notEqualLineProtocol =
                "test,testTagKey=diff testFieldKey=\"testFieldValue\" 1556813561098000000";
        final DataPoint notEqualDataPoint = InfluxParser.parseToDataPoint(notEqualLineProtocol);

        assertNotEquals(dataPoint, notEqualDataPoint);

        final String equalLineProtocol =
                "test,testTagKey=testTagValue testFieldKey=\"testFieldValue\" 1556813561098000000";
        final DataPoint equalDataPoint = InfluxParser.parseToDataPoint(equalLineProtocol);

        assertEquals(equalDataPoint, dataPoint);
    }

    @Test
    void shouldParseLineWithNoTimestamp() throws ParseException {
        final String lineProtocol = "test,testTagKey=testTagValue testFieldKey=\"testFieldValue\"";
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        assertNull(actualDataPoint.getTimestamp());
    }

    @ParameterizedTest
    @ValueSource(strings = {"t", "T", "true", "True", "TRUE"})
    void shouldParseLineWithFieldBoolAsTrueToPoint(final String fieldValue) throws ParseException {
        final String lineProtocol =
                String.format("test testFieldKey=%s 1556813561098000000", fieldValue);
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        assertEquals(actualDataPoint.getField("testFieldKey"), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"f", "F", "false", "False", "FALSE"})
    void shouldParseLineWithFieldBoolAsFalseToPoint(final String fieldValue) throws ParseException {
        final String lineProtocol =
                String.format("test testFieldKey=%s 1556813561098000000", fieldValue);
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        assertEquals(actualDataPoint.getField("testFieldKey"), false);
    }

    @Test
    void shouldParseLineWithFieldValueAsFloatToDataPoint() throws ParseException {
        final String lineProtocol = "test testFieldKey=-1.0 1556813561098000000";
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        final double expectedFieldValue = -1.0;
        assertEquals(expectedFieldValue, actualDataPoint.getField("testFieldKey"));
    }

    @Test
    void shouldParseLineWithFieldValueAsIntegerToDataPoint() throws ParseException {
        final String lineProtocol = "test testFieldKey=123456i 1556813561098000000";
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        final Long expectedFieldValue = 123456L;
        assertEquals(expectedFieldValue, actualDataPoint.getField("testFieldKey"));
    }

    @Test
    void shouldNotParseLineWithFieldValueAsUnsignedIntegerToDataPoint() {
        final String lineProtocol = "test testFieldKey=123u 1556813561098000000";

        assertThrows(
                ParseException.class,
                () -> InfluxParser.parseToDataPoint(lineProtocol),
                "Unable to parse line.");
    }

    @Test
    void shouldParseLineWithFieldValueAsStringToDataPoint() throws ParseException {
        final String lineProtocol = "test testFieldKey=\"testFieldValue\" 1556813561098000000";
        final DataPoint actualDataPoint = InfluxParser.parseToDataPoint(lineProtocol);

        assert actualDataPoint != null;
        assertEquals("testFieldValue", actualDataPoint.getField("testFieldKey"));
    }

    @Test
    void shouldNotParseLineWithNoMeasurement() {
        final String lineProtocol =
                "testTagKey=testTagValue testFieldKey=\"testFieldValue\" 1556813561098000000";

        assertThrows(
                ParseException.class,
                () -> InfluxParser.parseToDataPoint(lineProtocol),
                "Unable to parse line.");
    }

    @Test
    void shouldNotParseLineWithNoFieldSet() {
        final String lineProtocol = "test,testTagKey=123u 1556813561098000000";

        assertThrows(
                ParseException.class,
                () -> InfluxParser.parseToDataPoint(lineProtocol),
                "Unable to parse line.");
    }
}
