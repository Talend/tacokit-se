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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import org.talend.sdk.component.api.record.Record;

/**
 * Help to write record to an output.
 */
public class RecordWriter {

    /** to serialize record to an array of byte. */
    private final RecordSerializer serializer;

    public RecordWriter(RecordSerializer serializer) {
        this.serializer = serializer;
    }

    public void write(WritableByteChannel out, Record record) throws IOException {
        final byte[] recordContent = this.serializer.serialize(record);
        out.write(ByteBuffer.wrap(recordContent));
    }

    public void write(OutputStream out, Record record) throws IOException {
        final byte[] recordContent = this.serializer.serialize(record);
        out.write(recordContent);
    }

    public void write(WritableByteChannel out, Iterable<Record> records) throws IOException {
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        // put records in buffer.
        for (Record rec : records) {
            final byte[] recordContent = serializer.serialize(rec);
            buffer.write(recordContent);
        }
        // write buffer
        out.write(ByteBuffer.wrap(buffer.toByteArray()));
    }

    public void write(OutputStream out, Iterable<Record> records) throws IOException {
        for (Record rec : records) {
            this.write(out, rec);
        }
    }
}
