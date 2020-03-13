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
package org.talend.components.adlsgen2.common.format.avro;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Iterator;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroIterator implements Iterator<Record>, Serializable {

    private RecordBuilderFactory recordBuilderFactory;

    private final AvroConverter converter;

    private DataFileStream<GenericRecord> reader;

    private AvroIterator(AvroConverter converter, InputStream inputStream) {
        this.converter = converter;
        try {
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
            reader = new DataFileStream<GenericRecord>(inputStream, datumReader);
        } catch (IOException e) {
            log.error("[AvroIterator] {}", e.getMessage());
            throw new FileFormatRuntimeException(e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        if (reader.hasNext()) {
            return true;
        } else {
            try {
                reader.close();
                return false;
            } catch (IOException e) {
                throw new FileFormatRuntimeException(e.getMessage());
            }
        }
    }

    @Override
    public Record next() {
        GenericRecord current = reader.next();
        return current != null ? converter.toRecord(current) : null;
    }

    /**
     *
     */
    public static class Builder {

        private AvroConverter converter;

        private AvroConfiguration configuration;

        private RecordBuilderFactory factory;

        private Builder(final RecordBuilderFactory factory) {
            this.factory = factory;
        }

        public static AvroIterator.Builder of(RecordBuilderFactory factory) {
            return new AvroIterator.Builder(factory);
        }

        public AvroIterator.Builder withConfiguration(@Configuration("avroConfiguration") final AvroConfiguration configuration) {
            this.configuration = configuration;
            converter = AvroConverter.of(factory, configuration);

            return this;
        }

        public AvroIterator parse(InputStream in) {
            return new AvroIterator(converter, in);
        }

        public AvroIterator parse(String content) {
            throw new UnsupportedOperationException("#parse(String content)");
        }
    }

}
