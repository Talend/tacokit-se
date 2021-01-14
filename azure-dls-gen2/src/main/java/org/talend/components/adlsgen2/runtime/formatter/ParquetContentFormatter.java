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
package org.talend.components.adlsgen2.runtime.formatter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParquetContentFormatter extends AbstractContentFormatter {

    protected final ParquetConverter converter;

    private final RecordBuilderFactory recordBuilderFactory;

    private final OutputConfiguration configuration;

    public ParquetContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.configuration = configuration;
        converter = ParquetConverter.of(recordBuilderFactory, configuration.getDataSet().getParquetConfiguration());
    }

    @Override
    public byte[] feedContent(List<Record> records) {
        Schema avroSchema = converter.inferAvroSchema(records.get(0).getSchema());
        File tempFilePath = null;
        try {
            tempFilePath = File.createTempFile("adslgen2-tempFile", ".parquet");
            Path tempFile = new org.apache.hadoop.fs.Path(tempFilePath.getPath());
            ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord> builder(tempFile)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE).withSchema(avroSchema).build();
            for (Record r : records) {
                writer.write(converter.fromRecord(r));
            }
            writer.close();
            return Files.readAllBytes(tempFilePath.toPath());
        } catch (IOException e) {
            log.error("[feedContent] {}", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage());
        } finally {
            if (tempFilePath != null) {
                tempFilePath.delete();
            }
        }
    }

}
