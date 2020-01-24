/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.common.stream.api.output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import javax.json.Json;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.impl.RecordWriterStream;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

class RecordWriterStreamTest {

    @Test
    public void add() throws IOException {

        RecordConverter<byte[], byte[]> converter = new RecordConverter<byte[], byte[]>() {

            @Override
            public byte[] fromRecord(Record record) {
                return "{ \"field\" : \"value\" }".getBytes();
            }

            @Override
            public byte[] fromRecordSchema(Schema record) {
                return "".getBytes();
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FormatWriter<String> formatWriter = new FormatWriter<String>() {

            @Override
            public byte[] start(String config, Record first) {
                return ("[" + System.lineSeparator()).getBytes();
            }

            @Override
            public byte[] between(String config) {
                return ("," + System.lineSeparator()).getBytes();
            }

            @Override
            public byte[] end(String config) {
                return ("]" + System.lineSeparator()).getBytes();
            }
        };

        RecordWriterStream<String> writer = new RecordWriterStream<>(converter, out, formatWriter, "config");
        writer.add((Record) null);
        writer.add((Record) null);
        writer.flush();

        writer.add(Arrays.asList((Record) null, (Record) null));
        writer.end();

        String result = out.toString();
        JsonValue v = Json.createParser(new ByteArrayInputStream(result.getBytes())).getValue();
        Assertions.assertEquals(ValueType.ARRAY, v.getValueType());

    }

}