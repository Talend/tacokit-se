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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import javax.json.JsonBuilderFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class ParquetBlobWriter extends BlobWriter {

    private ParquetConverter converter;

    protected static final String EXT_PARQUET = ".parquet";

    public ParquetBlobWriter(OutputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            JsonBuilderFactory jsonFactory, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
        this.converter = ParquetConverter.of(recordBuilderFactory, configuration.getDataSet().getParquetConfiguration());
    }

    @Override
    protected void generateFile() {
        generateFileWithExtension(EXT_PARQUET);
    }

    @Override
    public void flush() {
        if (getBatch().isEmpty()) {
            return;
        }
        File tempFilePath = null;
        try {
            tempFilePath = File.createTempFile("tempFile", EXT_PARQUET);
            Path tempFile = new Path(tempFilePath.getPath());
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord> builder(tempFile)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(converter.inferAvroSchema(getSchema())).build();
            for (Record r : getBatch()) {
                writer.write(converter.fromRecord(r));
            }
            writer.close();
            uploadContent(Files.readAllBytes(tempFilePath.toPath()));
        } catch (IOException e) {
            throw new AdlsGen2RuntimeException(e.getMessage());
        } finally {
            getBatch().clear();
            currentItem.setBlobPath("");
            if (tempFilePath != null) {
                tempFilePath.delete();
            }
        }
    }
}
