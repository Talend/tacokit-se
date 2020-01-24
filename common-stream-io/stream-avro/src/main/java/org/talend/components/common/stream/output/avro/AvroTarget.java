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
import java.io.OutputStream;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.talend.components.common.stream.api.output.WritableTarget;

public class AvroTarget implements WritableTarget<GenericRecord> {

    private DataFileWriter<GenericRecord> dataFileWriter = null;

    private final OutputStream output;

    public AvroTarget(OutputStream output) {
        this.output = output;
    }

    @Override
    public void write(GenericRecord avroRecord) throws IOException {
        this.init(avroRecord);
        dataFileWriter.append(avroRecord);
    }

    @Override
    public void flush() throws IOException {
        if (dataFileWriter != null) {
            dataFileWriter.flush();
        }
    }

    @Override
    public void close() throws Exception {
        if (dataFileWriter != null) {
            this.flush();
            this.dataFileWriter.close();
        }
    }

    private void init(GenericRecord avroRecord) throws IOException {
        if (this.dataFileWriter == null) {
            final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
            this.dataFileWriter = new DataFileWriter<>(datumWriter);

            this.dataFileWriter.create(avroRecord.getSchema(), output);
        }
    }
}
