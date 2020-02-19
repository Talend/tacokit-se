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
package org.talend.components.azure.runtime.input;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.AvroConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AvroBlobFileReader extends BlobFileReader {

    public AvroBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices, messageService);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new AvroFileRecordIterator(blobItems, getRecordBuilderFactory());
    }

    private class AvroFileRecordIterator extends ItemRecordIterator<GenericRecord> {

        private AvroConverter converter;

        private DataFileStream<GenericRecord> avroItemIterator;

        private InputStream input;

        private AvroFileRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(GenericRecord next) {
            if (converter == null) {
                converter = AvroConverter.of(getRecordBuilderFactory());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            closePreviousInputStream();

            try {
                input = getCurrentItem().openInputStream();
                DatumReader<GenericRecord> reader = new GenericDatumReader<>();
                avroItemIterator = new DataFileStream<>(input, reader);
            } catch (Exception e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return avroItemIterator.hasNext();
        }

        @Override
        protected GenericRecord takeNextRecord() {
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
                    log.warn("Can't close stream", e);
                }
            }
        }
    }
}
