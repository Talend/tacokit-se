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

import java.util.Iterator;
import java.util.Map;

import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsDatasetRuntimeInfo;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BlobReader {

    protected RecordBuilderFactory recordBuilderFactory;

    private RecordIterator iterator;

    protected InputConfiguration configuration;

    protected final AdlsGen2Service service;

    protected final AdlsActiveDirectoryService tokenProviderService;

    protected final AdlsDatasetRuntimeInfo datasetRuntimeInfo;

    public BlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory, AdlsGen2Service service,
            AdlsActiveDirectoryService tokenProviderService) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.configuration = configuration;
        this.service = service;
        this.tokenProviderService = tokenProviderService;

        datasetRuntimeInfo = new AdlsDatasetRuntimeInfo(configuration.getDataSet(), tokenProviderService);
        Iterable<BlobInformations> blobItems = service.getBlobs(datasetRuntimeInfo);
        iterator = initRecordIterator(blobItems);
    }

    protected abstract RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems);

    public Record readRecord() {
        return iterator.next();
    }

    public static class BlobFileReaderFactory {

        private static JsonBuilderFactory jsonFactory;

        public static BlobReader getReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
                JsonBuilderFactory jsonFactory, AdlsGen2Service service, AdlsActiveDirectoryService tokenProviderService) {
            switch (configuration.getDataSet().getFormat()) {
            case CSV:
                return new CsvBlobReader(configuration, recordBuilderFactory, service, tokenProviderService);
            case AVRO:
                return new AvroBlobReader(configuration, recordBuilderFactory, service, tokenProviderService);
            case PARQUET:
                return new ParquetBlobReader(configuration, recordBuilderFactory, service, tokenProviderService);
            case JSON:
                return new JsonBlobReader(configuration, recordBuilderFactory, jsonFactory, service, tokenProviderService);
            default:
                throw new IllegalArgumentException("Unsupported file format"); // shouldn't be here
            }
        }
    }

    protected abstract class RecordIterator<T> implements Iterator<Record> {

        private Iterator<BlobInformations> blobList;

        @Getter(AccessLevel.PROTECTED)
        private RecordBuilderFactory recordBuilderFactory;

        @Getter(AccessLevel.PROTECTED)
        private BlobInformations currentBlob;

        protected RecordIterator(Iterable<BlobInformations> blobList, RecordBuilderFactory recordBuilderFactory) {
            this.blobList = blobList.iterator();
            this.recordBuilderFactory = recordBuilderFactory;
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("Use next() method until return null");
        }

        @Override
        public Record next() {
            T next = nextBlobRecord();

            return next != null ? convertToRecord(next) : null;
        }

        T nextBlobRecord() {
            if (currentBlob == null) {
                return null; // No items exists
            }

            if (hasNextBlobRecord()) {
                return peekNextBlobRecord();
            }

            while (blobList.hasNext()) {
                currentBlob = blobList.next();
                readBlob();
                if (hasNextBlobRecord()) {
                    return peekNextBlobRecord(); // read record from next item
                }
            }
            complete();
            return null;

        }

        protected abstract T peekNextBlobRecord();

        protected abstract boolean hasNextBlobRecord();

        protected abstract Record convertToRecord(T next);

        protected abstract void readBlob();

        protected void peekFirstBlob() {
            if (blobList.hasNext()) {
                currentBlob = blobList.next();
                readBlob();
            }
        }

        /**
         * Release all open resources if needed
         */
        protected abstract void complete();
    }
}
