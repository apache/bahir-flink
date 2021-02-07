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

import java.text.ParseException;
import java.util.List;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.TokenStream;
import org.apache.druid.data.input.influx.InfluxLineProtocolLexer;
import org.apache.druid.data.input.influx.InfluxLineProtocolParser;
import org.apache.druid.data.input.influx.InfluxLineProtocolParser.TimestampContext;

/**
 * This is an InfluxDB line protocol parser.
 *
 * @see <a href=https://docs.influxdata.com/influxdb/v2.0/reference/syntax/line-protocol/">Line
 *     Protocol</a>
 * @see <a
 *     href=https://github.com/apache/druid/blob/master/extensions-contrib/influx-extensions/src/main/java/org/apache/druid/data/input/influx/InfluxParser.java>
 *     Apache Druid InfluxDB Parser </a>
 */
public class InfluxParser {
    private static final Pattern BACKSLASH_PATTERN = Pattern.compile("\\\\\"");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("\\\\([,= ])");

    @Nullable
    public DataPoint parseToDataPoint(final String input) throws ParseException {
        final CharStream charStream = new ANTLRInputStream(input);
        final InfluxLineProtocolLexer lexer = new InfluxLineProtocolLexer(charStream);
        final TokenStream tokenStream = new CommonTokenStream(lexer);
        final InfluxLineProtocolParser parser = new InfluxLineProtocolParser(tokenStream);

        final List<InfluxLineProtocolParser.LineContext> lines = parser.lines().line();
        if (parser.getNumberOfSyntaxErrors() != 0) {
            throw new ParseException("Unable to parse line.", 0);
        }
        if (lines.size() != 1) {
            throw new ParseException(
                    "Multiple lines present; unable to parse more than one per record.", 0);
        }

        final InfluxLineProtocolParser.LineContext line = lines.get(0);
        final String measurement = this.parseIdentifier(line.identifier());

        final Number timestamp = this.parseTimestamp(line.timestamp());

        final DataPoint out = new DataPoint(measurement, timestamp);

        if (line.tag_set() != null) {
            line.tag_set().tag_pair().forEach(t -> this.parseTag(t, out));
        }

        line.field_set().field_pair().forEach(t -> this.parseField(t, out));

        return out;
    }

    private void parseTag(final InfluxLineProtocolParser.Tag_pairContext tag, final DataPoint out) {
        final String key = this.parseIdentifier(tag.identifier(0));
        final String value = this.parseIdentifier(tag.identifier(1));
        out.addTag(key, value);
    }

    private void parseField(
            final InfluxLineProtocolParser.Field_pairContext field, final DataPoint out) {
        final String key = this.parseIdentifier(field.identifier());
        final InfluxLineProtocolParser.Field_valueContext valueContext = field.field_value();
        final Object value;
        if (valueContext.NUMBER() != null) {
            value = this.parseNumber(valueContext.NUMBER().getText());
        } else if (valueContext.BOOLEAN() != null) {
            value = this.parseBool(valueContext.BOOLEAN().getText());
        } else {
            value = this.parseQuotedString(valueContext.QUOTED_STRING().getText());
        }
        out.addField(key, value);
    }

    private Object parseQuotedString(final String text) {
        return BACKSLASH_PATTERN.matcher(text.substring(1, text.length() - 1)).replaceAll("\"");
    }

    private Object parseNumber(final String raw) {
        if (raw.endsWith("i")) {
            return Long.valueOf(raw.substring(0, raw.length() - 1));
        }

        return new Double(raw);
    }

    private Object parseBool(final String raw) {
        final char first = raw.charAt(0);
        if (first == 't' || first == 'T') {
            return "true";
        } else {
            return "false";
        }
    }

    private String parseIdentifier(final InfluxLineProtocolParser.IdentifierContext ctx) {
        if (ctx.BOOLEAN() != null || ctx.NUMBER() != null) {
            return ctx.getText();
        }

        return IDENTIFIER_PATTERN.matcher(ctx.IDENTIFIER_STRING().getText()).replaceAll("$1");
    }

    private Number parseTimestamp(@Nullable final TimestampContext timestamp) {
        if (timestamp != null) {
            final String strTimestamp = timestamp.getText();
            // Influx timestamps come in nanoseconds; treat anything less than 1 ms as 0
            if (strTimestamp.length() < 7) {
                return 0L;
            } else {
                return Long.valueOf(strTimestamp.substring(0, strTimestamp.length() - 6));
            }
        }
        return null;
    }
}
