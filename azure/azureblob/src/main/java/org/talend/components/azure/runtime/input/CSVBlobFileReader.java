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
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.azure.common.Encoding;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.CSVConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVBlobFileReader extends BlobFileReader {

    CSVBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices, messageService);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new CSVFileRecordIterator(blobItems, getRecordBuilderFactory());
    }

    @Override
    public Record readRecord() {
        return super.readRecord();
    }

    private class CSVFileRecordIterator extends ItemRecordIterator<CSVRecord> {

        private InputStream currentItemInputStream;

        private Iterator<CSVRecord> fileRecordIterator;

        private CSVFormat format;

        private CSVConverter converter;

        private String encodingValue;

        private CSVFileRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            this.encodingValue = getConfig().getCsvOptions().getEncoding() == Encoding.OTHER
                    ? getConfig().getCsvOptions().getCustomEncoding()
                    : getConfig().getCsvOptions().getEncoding().getEncodingValue();

            takeFirstItem();
        }

        @Override
        protected Record convertToRecord(CSVRecord next) {
            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            initMetadataIfNeeded();

            closePreviousInputStream();

            try {
                currentItemInputStream = getCurrentItem().openInputStream();

                InputStreamReader inr = new InputStreamReader(currentItemInputStream, encodingValue);
                CSVParser parser = new CSVParser(inr, format);
                fileRecordIterator = parser.iterator();
                if (fileRecordIterator.hasNext() && getConfig().getCsvOptions().isUseHeader()
                        && getConfig().getCsvOptions().getHeader() >= 1) {
                    for (int i = 0; i < getConfig().getCsvOptions().getHeader() - 1; i++) {
                        if (fileRecordIterator.hasNext()) {
                            // skip extra header lines
                            fileRecordIterator.next();
                        }
                    }

                    if (fileRecordIterator.hasNext()) {
                        CSVRecord headerRecord = fileRecordIterator.next();
                        // save schema from first file
                        if (converter.getSchema() == null) {
                            converter.toRecord(headerRecord);
                        }
                    }
                }
            } catch (Exception e) {
                throw new BlobRuntimeException(e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return fileRecordIterator.hasNext();
        }

        @Override
        protected CSVRecord takeNextRecord() {
            return fileRecordIterator.next();
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void initMetadataIfNeeded() {
            if (converter == null) {
                converter = CSVConverter.of(getRecordBuilderFactory(), getConfig().getCsvOptions());
            }

            if (format == null) {
                format = converter.getCsvFormat();
            }
        }

        private void closePreviousInputStream() {
            if (currentItemInputStream != null) {
                try {
                    currentItemInputStream.close();
                } catch (IOException e) {
                    log.warn("Can't close stream", e);
                }
            }
        }
    }
}
