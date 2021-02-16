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
package org.talend.components.adlsgen2.runtime.input;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.components.common.converters.AvroConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroBlobReader extends BlobReader {

    public AvroBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory, AdlsGen2Service service,
            AdlsActiveDirectoryService activeDirectoryService) {
        super(configuration, recordBuilderFactory, service, activeDirectoryService);
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new AvroFileRecordIterator(blobItems, recordBuilderFactory);
    }

    private class AvroFileRecordIterator extends RecordIterator<GenericRecord> {

        private AvroConverter converter;

        private DataFileStream<GenericRecord> avroItemIterator;

        private InputStream input;

        private AvroFileRecordIterator(Iterable<BlobInformations> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            peekFirstBlob();
        }

        @Override
        protected Record convertToRecord(GenericRecord next) {
            if (converter == null) {
                converter = AvroConverter.of(getRecordBuilderFactory(), configuration.getDataSet().getAvroConfiguration());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readBlob() {
            closePreviousInputStream();
            try {
                input = service.getBlobInputstream(datasetRuntimeInfo, getCurrentBlob());
                DatumReader<GenericRecord> reader = new GenericDatumReader<>();
                avroItemIterator = new DataFileStream<>(input, reader);
            } catch (Exception e) {
                throw new AdlsGen2RuntimeException(e.getMessage());
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return avroItemIterator.hasNext();
        }

        @Override
        protected GenericRecord peekNextBlobRecord() {
            return avroItemIterator.next();
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void closePreviousInputStream() {
            if (avroItemIterator != null) {
                try {
                    avroItemIterator.close();
                    input.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }
}
