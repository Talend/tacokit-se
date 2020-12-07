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
package org.talend.components.adlsgen2.runtime.output;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.json.JsonBuilderFactory;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.talend.components.adlsgen2.common.format.avro.AvroConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroBlobWriter extends BlobWriter {

    public final String EXT_AVRO = ".avro";

    private AvroConverter converter;

    public AvroBlobWriter(OutputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactory, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        converter = AvroConverter.of(recordBuilderFactory, configuration.getDataSet().getAvroConfiguration());
    }

    @Override
    public void generateFile() {
        generateFileWithExtension(EXT_AVRO);
    }

    @Override
    public void flush() {
        if (getBatch().isEmpty()) {
            return;
        }
        byte[] contents = convertBatchToBytes();
        uploadContent(contents);
        getBatch().clear();
        currentItem.setBlobPath("");
    }

    private byte[] convertBatchToBytes() {
        try {
            ByteArrayOutputStream byteBuffer = new ByteArrayOutputStream();
            DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>();
            DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
            dataFileWriter.create(converter.inferAvroSchema(getSchema()), byteBuffer);
            for (Record record : getBatch()) {
                dataFileWriter.append(converter.fromRecord(record));
            }
            dataFileWriter.flush();
            return byteBuffer.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
