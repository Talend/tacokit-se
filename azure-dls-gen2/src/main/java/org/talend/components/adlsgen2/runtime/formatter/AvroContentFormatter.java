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
package org.talend.components.adlsgen2.runtime.formatter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.talend.components.adlsgen2.common.format.avro.AvroConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class AvroContentFormatter extends AbstractContentFormatter {

    private final RecordBuilderFactory recordBuilderFactory;

    private final OutputConfiguration configuration;

    private final AvroConverter converter;

    public AvroContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.configuration = configuration;
        converter = AvroConverter.of(recordBuilderFactory, configuration.getDataSet().getAvroConfiguration());
    }

    @Override
    public byte[] feedContent(List<Record> records) {
        try {
            Schema schema = records.get(0).getSchema();
            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(converter.inferAvroSchema(schema), byteBuffer);
            for (Record record : records) {
                dataFileWriter.append(converter.fromRecord(record));
            }
            dataFileWriter.flush();
            return byteBuffer.toByteArray();
        } catch (IOException e) {
            throw new AdlsGen2RuntimeException(e.getMessage());
        }
    }

}
