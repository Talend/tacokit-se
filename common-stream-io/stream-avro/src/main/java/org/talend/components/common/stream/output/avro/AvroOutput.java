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
package org.talend.components.common.stream.output.avro;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.avro.AvroConfiguration;

/**
 * Writer for avro record to output.
 */
public abstract class AvroOutput implements AutoCloseable {

    /** indicate if first message */
    private boolean first = true;

    private final TargetFinder destination;

    public static AvroOutput buildOutput(final AvroConfiguration avroConfig, final TargetFinder destinationFinder) {
        if (avroConfig.isAttachSchema()) {
            return new WithHead(destinationFinder);
        } else {
            final Schema schema = avroConfig.getAvroSchema() != null ? new Schema.Parser().parse(avroConfig.getAvroSchema())
                    : null;
            return new Headless(destinationFinder, schema);
        }
    }

    public AvroOutput(final TargetFinder destination) {
        this.destination = destination;
    }

    public final void write(final GenericRecord avroRecord) throws IOException {
        if (first) {
            this.first(avroRecord);
            first = false;
        }
        this.doWrite(avroRecord);
    }

    protected final OutputStream destination() throws IOException {
        return this.destination.find();
    }

    protected abstract void doWrite(final GenericRecord avroRecord) throws IOException;

    protected abstract void first(final GenericRecord firstRecord) throws IOException;

    public abstract void flush() throws IOException;

    @Override
    public abstract void close() throws IOException;

    private static class WithHead extends AvroOutput {

        private final DataFileWriter<GenericRecord> dataFileWriter;

        public WithHead(final TargetFinder destination) {
            super(destination);
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
            this.dataFileWriter = new DataFileWriter<>(datumWriter);
        }

        @Override
        protected void doWrite(GenericRecord avroRecord) throws IOException {
            this.dataFileWriter.append(avroRecord);
        }

        @Override
        protected void first(GenericRecord firstRecord) throws IOException {
            this.dataFileWriter.create(firstRecord.getSchema(), this.destination());
        }

        @Override
        public void flush() throws IOException {
            this.dataFileWriter.flush();
        }

        @Override
        public void close() throws IOException {
            this.dataFileWriter.close();
        }
    }

    private static class Headless extends AvroOutput {

        private final GenericDatumWriter<GenericRecord> datumWriter;

        private BinaryEncoder out;

        public Headless(final TargetFinder destination, Schema schema) {
            super(destination);
            datumWriter = new GenericDatumWriter<>(schema);
        }

        @Override
        protected void doWrite(GenericRecord avroRecord) throws IOException {
            this.datumWriter.write(avroRecord, out);
        }

        @Override
        protected void first(GenericRecord firstRecord) throws IOException {
            this.out = EncoderFactory.get().binaryEncoder(destination(), null);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            out.flush();
        }
    }
}
