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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.impl.RecordWriterChannel;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

class RecordWriterTest {

    private static final String recordString = "unusefull tests datas [...............]\n";

    @Test
    void testCollectionOutputStream() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        FormatWriter<String> fw = new FormatWriter<String>() {
        };

        final RecordByteWriter writer = new RecordByteWriter<>(new TestRecConverter(), fw, "", () -> out);

        final List<Record> records = Arrays.asList((Record) null, (Record) null, (Record) null);
        writer.add(records);

        String res = out.toString();
        Assertions.assertEquals(res.length(), RecordWriterTest.recordString.length() * 3);
    }

    @Test
    void testSpecific() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        FormatWriter<String> fw = new FormatWriter<String>() {

            @Override
            public byte[] start(String config, Record record) {
                return "{".getBytes();
            }

            @Override
            public byte[] between(String config) {
                return ",".getBytes();
            }

            @Override
            public byte[] end(String config) {
                return "}".getBytes();
            }
        };

        final RecordByteWriter writer = new RecordByteWriter<>(new TestRecConverter("record"), fw, "", () -> out);

        final List<Record> records = Arrays.asList((Record) null, (Record) null, (Record) null);
        writer.add(records);
        writer.end();
        String res = out.toString();
        Assertions.assertEquals("{record,record,record}", res);
    }

    private byte[] serialize(Record record) {
        return RecordWriterTest.recordString.getBytes(Charset.defaultCharset());
    }

    static class TestRecConverter implements RecordConverter<byte[], byte[]> {

        final String data;

        public TestRecConverter() {
            this(RecordWriterTest.recordString);
        }

        public TestRecConverter(String data) {
            this.data = data;
        }

        @Override
        public byte[] fromRecord(Record record) {
            return data.getBytes();
        }

        @Override
        public byte[] fromRecordSchema(Schema record) {
            return "Schema".getBytes();
        }
    }

    static class TestWritableChannel implements WritableByteChannel {

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public int write(ByteBuffer src) throws IOException {
            final byte[] array = src.array();
            out.write(array);
            return array.length;
        }

        public String getContent() {
            return out.toString();
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {
        }
    }
}