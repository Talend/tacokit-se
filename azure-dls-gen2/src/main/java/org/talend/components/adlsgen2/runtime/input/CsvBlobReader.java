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
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.adlsgen2.common.format.csv.CsvConverter;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvBlobReader extends BlobReader {

    CsvBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory, AdlsGen2Service service,
            AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, service, tokenProviderService);
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new CSVFileRecordIterator(blobItems, recordBuilderFactory, service);
    }

    @Override
    public Record readRecord() {
        return super.readRecord();
    }

    private class CSVFileRecordIterator extends RecordIterator<CSVRecord> {

        private InputStream currentItemInputStream;

        private Iterator<CSVRecord> csvRecordIterator;

        private CSVFormat format;

        private CsvConverter converter;

        private String encodingValue;

        private AdlsGen2Service service;

        private CSVParser parser;

        private CSVFileRecordIterator(Iterable<BlobInformations> blobList, RecordBuilderFactory recordBuilderFactory,
                AdlsGen2Service service) {
            super(blobList, recordBuilderFactory);
            this.encodingValue = configuration.getDataSet().getCsvConfiguration().effectiveFileEncoding();
            this.service = service;
            peekFirstBlob();
        }

        @Override
        protected Record convertToRecord(CSVRecord next) {
            return converter.toRecord(next);
        }

        @Override
        protected void readBlob() {
            initMetadataIfNeeded();
            closePreviousInputStream();
            try {
                currentItemInputStream = service.getBlobInputstream(datasetRuntimeInfo, getCurrentBlob());
                InputStreamReader inr = new InputStreamReader(currentItemInputStream, encodingValue);
                parser = new CSVParser(inr, format);
                converter.setRuntimeHeaders(parser.getHeaderMap());
                csvRecordIterator = parser.iterator();
            } catch (Exception e) {
                throw new AdlsGen2RuntimeException(e.getMessage());
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return csvRecordIterator.hasNext();
        }

        @Override
        protected CSVRecord peekNextBlobRecord() {
            return csvRecordIterator.next();
        }

        @Override
        protected void complete() {
            closePreviousInputStream();
        }

        private void initMetadataIfNeeded() {
            if (converter == null) {
                converter = CsvConverter.of(getRecordBuilderFactory(), configuration.getDataSet().getCsvConfiguration());
            }

            if (format == null) {
                format = converter.getCsvFormat();
            }
        }

        private void closePreviousInputStream() {
            if (currentItemInputStream != null) {
                try {
                    currentItemInputStream.close();
                    parser.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }
}
