/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.output.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.sdk.component.api.record.Record;

import java.io.IOException;

public class HeadlessAvroRecordWriter implements RecordWriter {

    /** convert talend record to Avro. */
    private final RecordConverter<GenericRecord, Schema> converter;

    /** output stream finder */
    private final TargetFinder destination;

    /** avro writer */
    private final GenericDatumWriter<GenericRecord> datumWriter;

    private BinaryEncoder out;

    private boolean first = true;

    public HeadlessAvroRecordWriter(RecordConverter<GenericRecord, org.apache.avro.Schema> converter, TargetFinder destination,
            Schema schema) {
        this.converter = converter;
        this.destination = destination;

        datumWriter = new GenericDatumWriter<>(schema);
    }

    @Override
    public void add(Record record) throws IOException {
        if (first) {
            out = EncoderFactory.get().binaryEncoder(destination.find(), null);
            first = false;
        }

        final GenericRecord avroRecord = converter.fromRecord(record);
        datumWriter.write(avroRecord, out);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
