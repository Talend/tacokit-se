/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.input.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.sdk.component.api.record.Record;

public class AvroReader implements RecordReader {

    private final AvroToRecord convertor;

    private DataFileStream<GenericRecord> genericRecordReader = null;

    public AvroReader(AvroToRecord convertor) {
        this.convertor = convertor;
    }

    @Override
    public Iterator<Record> read(InputStream input) {

        try {
            final DatumReader<GenericRecord> userDatumReader = new GenericDatumReader<>();
            this.genericRecordReader = new DataFileStream<GenericRecord>(input, userDatumReader);

            return new IteratorMap<>(this.genericRecordReader, this.convertor::toRecord);
        } catch (IOException exIO) {
            throw new UncheckedIOException("Unable to open avro reader : " + exIO.getMessage(), exIO);
        }
    }

    @Override
    public void close() {
        if (this.genericRecordReader != null) {
            try {
                this.genericRecordReader.close();
            } catch (IOException exIO) {
                throw new UncheckedIOException("Unable to close avro reader : " + exIO.getMessage(), exIO);
            } finally {
                this.genericRecordReader = null;
            }
        }
    }
}
