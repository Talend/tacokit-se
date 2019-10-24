/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.talend.components.azure.common.Protocol;
import org.talend.components.azure.dataset.AzureBlobDataset;
import org.talend.components.azure.runtime.converters.ParquetConverter;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.ListBlobItem;
import lombok.extern.slf4j.Slf4j;
import static org.talend.components.azure.common.service.AzureComponentServices.SAS_PATTERN;

@Slf4j
public class ParquetBlobFileReader extends BlobFileReader {

    public ParquetBlobFileReader(AzureBlobDataset config, RecordBuilderFactory recordBuilderFactory,
            AzureBlobComponentServices connectionServices, MessageService messageService)
            throws URISyntaxException, StorageException {
        super(config, recordBuilderFactory, connectionServices, messageService);
    }

    @Override
    protected ItemRecordIterator initItemRecordIterator(Iterable<ListBlobItem> blobItems) {
        return new ParquetRecordIterator(blobItems, getRecordBuilderFactory());
    }

    private class ParquetRecordIterator extends ItemRecordIterator<GenericRecord> {

        private final static String AZURE_FILESYSTEM_PROPERTY_KEY = "fs.azure";

        private final static String AZURE_FILESYSTEM_PROPERTY_VALUE = "org.apache.hadoop.fs.azure.NativeAzureFileSystem";

        private final static String AZURE_ACCOUNT_CRED_KEY_FORMAT = "fs.azure.account.key.%s.blob.core.windows.net";

        private final static String AZURE_SAS_CRED_KEY_FORMAT = "fs.azure.sas.%s.%s.blob.core.windows.net";

        private final static String AZURE_URI_FORMAT = "wasb%s://%s@%s.blob.core.windows.net/%s";

        private Pattern sasPattern = Pattern.compile(SAS_PATTERN);

        private ParquetConverter converter;

        private Configuration hadoopConfig;

        private String accountName;

        private ParquetReader<GenericRecord> reader;

        private GenericRecord currentRecord;

        private ParquetRecordIterator(Iterable<ListBlobItem> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            initConfig();
            takeFirstItem();
        }

        private void initConfig() {
            hadoopConfig = new Configuration();
            hadoopConfig.set(AZURE_FILESYSTEM_PROPERTY_KEY, AZURE_FILESYSTEM_PROPERTY_VALUE);
            if (getConfig().getConnection().isUseAzureSharedSignature()) {
                Matcher mather = sasPattern
                        .matcher(getConfig().getConnection().getSignatureConnection().getAzureSharedAccessSignature());
                accountName = mather.group(2);
                String sasKey = String.format(AZURE_SAS_CRED_KEY_FORMAT, getConfig().getContainerName(), accountName);
                String token = mather.group(4);
                hadoopConfig.set(sasKey, token);
            } else {
                accountName = getConfig().getConnection().getAccountConnection().getAccountName();
                String accountCredKey = String.format(AZURE_ACCOUNT_CRED_KEY_FORMAT, accountName);
                hadoopConfig.set(accountCredKey, getConfig().getConnection().getAccountConnection().getAccountKey());
            }
        }

        @Override
        protected Record convertToRecord(GenericRecord next) {
            if (converter == null) {
                converter = ParquetConverter.of(getRecordBuilderFactory());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readItem() {
            closePreviousInputStream();

            boolean isHttpsConnectionUsed = getConfig().getConnection().isUseAzureSharedSignature()
                    || getConfig().getConnection().getAccountConnection().getProtocol().equals(Protocol.HTTPS);
            String blobURI = String.format(AZURE_URI_FORMAT, isHttpsConnectionUsed ? "s" : "", getConfig().getContainerName(),
                    accountName, getCurrentItem().getName());
            try {
                InputFile file = HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(blobURI), hadoopConfig);
                reader = AvroParquetReader.<GenericRecord> builder(file).build();
                currentRecord = reader.read();
            } catch (IOException e) {
                log.error("Can't read item", e);
            }
        }

        @Override
        protected boolean hasNextRecordTaken() {
            return currentRecord != null;
        }

        @Override
        protected GenericRecord takeNextRecord() {
            GenericRecord currentRecord = this.currentRecord;
            try {
                // read next line for next method call
                this.currentRecord = reader.read();
            } catch (IOException e) {
                log.error("Can't read record from file " + getCurrentItem().getName(), e);
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
                    log.warn("Can't close stream", e);
                }
            }
        }
    }

}
