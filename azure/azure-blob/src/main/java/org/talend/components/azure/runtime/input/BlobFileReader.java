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
package org.talend.components.azure.runtime.input;

import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Iterator;

import org.talend.components.common.formats.excel.ExcelFormat;
import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.common.service.azureblob.AzureComponentServices;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.AccessLevel;
import lombok.Getter;

public abstract class BlobFileReader {

    private final MessageService messageService;

    @Getter(AccessLevel.PROTECTED)
    private RecordBuilderFactory recordBuilderFactory;

    private ItemRecordIterator iterator;

    private final AzureBlobDataset config;

    public BlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        this.recordBuilderFactory = recordBuilderFactory;
        this.config = config;
        this.messageService = messageService;
        CloudStorageAccount connection = connectionServices.createStorageAccount(config.getConnection());
        CloudBlobClient blobClient = connectionServices.getConnectionService().createCloudBlobClient(connection,
                AzureComponentServices.DEFAULT_RETRY_POLICY);
        CloudBlobContainer container = checkBlobContainer(config, blobClient);

        String directoryName = config.getDirectory();
        if (directoryName == null) {
            directoryName = "";
        } else if (!directoryName.endsWith("/")) {
            directoryName += "/";
        }

        Iterable<ListBlobItem> blobItems = container.listBlobs(directoryName, false, EnumSet.noneOf(BlobListingDetails.class),
                null, AzureComponentServices.getTalendOperationContext());
        if (!blobItems.iterator().hasNext()) {
            throw new BlobRuntimeException("Folder doesn't exist/is empty");
        }
        this.iterator = initItemRecordIterator(blobItems);
    }

    public CloudBlobContainer checkBlobContainer(AzureBlobDataset config, CloudBlobClient blobClient)
            throws URISyntaxException, StorageException {
        CloudBlobContainer container = blobClient.getContainerReference(config.getContainerName());

        try {
            if (!container.exists()) {
                throw new BlobRuntimeException(messageService.containerNotExist());
            }
        } catch (StorageException e) {
            throw new BlobRuntimeException(messageService.illegalContainerName(), e);
        }
        return container;
    }

    protected abstract ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems);

    public Record readRecord() {
        return iterator.next();
    }

    protected AzureBlobDataset getConfig() {
        return config;
    }

    public static class BlobFileReaderFactory {

        public static BlobFileReader getReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
                AzureBlobComponentServices connectionServices, MessageService messageService) throws Exception {
            switch (config.getFileFormat()) {
            case CSV:
                return new CSVBlobFileReader(config, recordBuilderFactory, connectionServices, messageService);
            case AVRO:
                return new AvroBlobFileReader(config, recordBuilderFactory, connectionServices, messageService);

            case EXCEL:
                if (config.getExcelOptions().getExcelFormat() == ExcelFormat.HTML) {
                    return new ExcelHTMLBlobFileReader(config, recordBuilderFactory, connectionServices, messageService);
                } else {
                    return new ExcelBlobFileReader(config, recordBuilderFactory, connectionServices, messageService);
                }
            case PARQUET:
                return new ParquetBlobFileReader(config, recordBuilderFactory, connectionServices, messageService);
            default:
                throw new IllegalArgumentException("Unsupported file format"); // shouldn't be here
            }
        }
    }

    protected abstract class ItemRecordIterator<T> implements Iterator<Record> {

        private Iterator<ListBlobItem> blobItems;

        @Getter(AccessLevel.PROTECTED)
        private RecordBuilderFactory recordBuilderFactory;

        @Getter(AccessLevel.PROTECTED)
        private CloudBlob currentItem;

        protected ItemRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            this.blobItems = blobItemsList.iterator();
            this.recordBuilderFactory = recordBuilderFactory;
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("Use next() method until return null");
        }

        @Override
        public Record next() {
            T next = readNextItemRecord();

            return next != null ? convertToRecord(next) : null;
        }

        T readNextItemRecord() {
            if (currentItem == null) {
                return null; // No items exists
            }

            if (hasNextRecordTaken()) {
                return takeNextRecord();
            }

            while (blobItems.hasNext()) {
                Object next = blobItems.next();
                if (next instanceof CloudBlob) {
                    currentItem = (CloudBlob) next;
                    readItem();
                    if (hasNextRecordTaken()) {
                        return takeNextRecord(); // read record from next item
                    }
                }
            }

            complete();
            return null;

        }

        protected abstract T takeNextRecord();

        protected abstract boolean hasNextRecordTaken();

        protected abstract Record convertToRecord(T next);

        protected abstract void readItem();

        protected void takeFirstItem() {
            while (blobItems.hasNext()) {
                Object next = blobItems.next();
                if (next instanceof CloudBlob) {
                    currentItem = (CloudBlob) next;
                    readItem();
                    break;
                }
            }
        }

        /**
         * Release all open resources if needed
         */
        protected abstract void complete();
    }
}
