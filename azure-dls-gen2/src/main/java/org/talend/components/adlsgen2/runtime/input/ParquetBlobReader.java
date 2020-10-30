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
package org.talend.components.adlsgen2.runtime.input;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
import org.talend.components.adlsgen2.common.format.parquet.ParquetConverter;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParquetBlobReader extends BlobReader {

    public ParquetBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            AdlsGen2Service connectionServices, AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, connectionServices, tokenProviderService);
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new ParquetRecordIterator(blobItems, recordBuilderFactory);
    }

    private class ParquetRecordIterator extends RecordIterator<GenericRecord> {

        private ParquetConverter converter;

        private Configuration hadoopConfig;

        private String accountName;

        private ParquetReader<GenericRecord> reader;

        private GenericRecord currentRecord;

        private ParquetRecordIterator(Iterable<BlobInformations> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            initConfig();
            peekFirstBlob();
        }

        private void initConfig() {
            hadoopConfig = new Configuration();
        }

        @Override
        protected Record convertToRecord(GenericRecord next) {
            if (converter == null) {
                converter = ParquetConverter.of(getRecordBuilderFactory(), configuration.getDataSet().getParquetConfiguration());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readBlob() {
            closePreviousInputStream();
            try {
                File tmp = File.createTempFile("talend-adls-gen2-tmp", ".parquet");
                tmp.deleteOnExit();
                InputStream input = service.getBlobInputstream(datasetRuntimeInfo, getCurrentBlob());
                Files.copy(input, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
                IOUtils.closeQuietly(input);
                HadoopInputFile hdpIn = HadoopInputFile.fromPath(new Path(tmp.getPath()),
                        new org.apache.hadoop.conf.Configuration());
                reader = AvroParquetReader.<GenericRecord> builder(hdpIn).build();
                currentRecord = reader.read();
            } catch (IOException e) {
                log.error("[ParquetIterator] {}", e.getMessage());
                throw new FileFormatRuntimeException(e.getMessage());
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return currentRecord != null;
        }

        @Override
        protected GenericRecord peekNextBlobRecord() {
            GenericRecord currentRecord = this.currentRecord;
            try {
                this.currentRecord = reader.read();
            } catch (IOException e) {
                log.error("Can't read record from file " + getCurrentBlob().getBlobPath(), e);
            }

            return currentRecord;
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void closePreviousInputStream() {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }

}
