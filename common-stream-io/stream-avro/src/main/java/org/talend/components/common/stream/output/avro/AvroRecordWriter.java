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
package org.talend.components.common.stream.output.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.sdk.component.api.record.Record;

public class AvroRecordWriter implements RecordWriter {

    private final RecordConverter<GenericRecord, org.apache.avro.Schema> converter;

    private final WritableTarget<GenericRecord> destination;

    public AvroRecordWriter(RecordConverter<GenericRecord, org.apache.avro.Schema> converter,
            WritableTarget<GenericRecord> destination) {
        this.converter = converter;
        this.destination = destination;
    }

    @Override
    public void add(Record record) throws IOException {
        final GenericRecord avroRecord = converter.fromRecord(record);
        this.destination.write(avroRecord);
    }

    @Override
    public void flush() throws IOException {
        this.destination.flush();
    }

    @Override
    public void close() throws Exception {
        this.destination.close();
    }
}
