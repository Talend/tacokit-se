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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
import org.talend.components.adlsgen2.common.format.delta.DeltaConverter;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.input.InputConfiguration;
import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.components.adlsgen2.service.BlobInformations;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.IOException;

import static org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;

@Slf4j
public class DeltaBlobReader extends BlobReader {

    public DeltaBlobReader(InputConfiguration configuration, RecordBuilderFactory recordBuilderFactory,
            AdlsGen2Service connectionServices, AdlsActiveDirectoryService tokenProviderService) {
        super(configuration, recordBuilderFactory, connectionServices, tokenProviderService);
    }

    @Override
    protected RecordIterator initRecordIterator(Iterable<BlobInformations> blobItems) {
        return new DeltaRecordIterator(blobItems, recordBuilderFactory);
    }

    private class DeltaRecordIterator extends RecordIterator<RowRecord> {

        private DeltaConverter converter;

        private Configuration hadoopConfig;

        private CloseableIterator<RowRecord> iter;

        private RowRecord currentRecord;

        private DeltaRecordIterator(Iterable<BlobInformations> blobItemsList, RecordBuilderFactory recordBuilderFactory) {
            super(blobItemsList, recordBuilderFactory);
            initConfig();
            peekFirstBlob();
        }

        private void initConfig() {
            hadoopConfig = new Configuration();

            AdlsGen2Connection datastore = datasetRuntimeInfo.getConnection();
            String accountName = datastore.getAccountName();
            AuthMethod authMethod = datastore.getAuthMethod();

            // TODO check if need to use endpoint as tail, for eaxmple : "dfs.core.windows.net"
            // TODO other auth way

            switch (authMethod) {
            case SharedKey:
                String sharedKey = datastore.getSharedKey();
                hadoopConfig.set("fs.azure.account.auth.type." + accountName + ".dfs.core.windows.net", "SharedKey");
                hadoopConfig.set("fs.azure.account.key." + accountName + ".dfs.core.windows.net", sharedKey);
                break;
            case SAS:
                // seems hadoop 3.2.2 don't support that, need to upgrade hadoop
                String sas = datastore.getSas();
                hadoopConfig.set("fs.azure.account.auth.type", "SAS");
                hadoopConfig.set("fs.azure.sas.token.provider.type",
                        "a org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider impl");
                break;
            case ActiveDirectory:
                String tenantId = datastore.getTenantId();
                String clientId = datastore.getClientId();
                String clientSecret = datastore.getClientSecret();
                break;
            default:
                break;
            }
        }

        @Override
        protected Record convertToRecord(RowRecord next) {
            if (converter == null) {
                converter = DeltaConverter.of(getRecordBuilderFactory());
            }

            return converter.toRecord(next);
        }

        @Override
        protected void readBlob() {
            closePreviousIterator();
            try {
                // TODO use getCurrentBlob() now? as delta format is a directory self with parquet files and json files in it, so
                // we need to list blob objects? i think no need, TODO check it
                StringBuilder strBuilder = new StringBuilder();
                strBuilder.append("abfss://").append(datasetRuntimeInfo.getDataSet().getFilesystem()).append('@')
                        .append(datasetRuntimeInfo.getConnection().getAccountName()).append('.')
                        .append(datasetRuntimeInfo.getConnection().getEndpointSuffix());
                if(!getCurrentBlob().getBlobPath().startsWith("/")) {
                    strBuilder.append("/");
                }
                strBuilder.append(getCurrentBlob().getBlobPath());

                DeltaLog log = DeltaLog.forTable(hadoopConfig, strBuilder.toString());
                Snapshot snapshot = log.snapshot();
                iter = snapshot.open();

                this.currentRecord = nextRecord();
            } catch (Exception e) {
                log.error("[DeltaIterator] {}", e.getMessage());
                throw new FileFormatRuntimeException(e.getMessage());
            }
        }

        @Override
        protected boolean hasNextBlobRecord() {
            return currentRecord != null;
        }

        private RowRecord nextRecord() {
            if (iter.hasNext()) {
                RowRecord row = iter.next();
                return row;
            }

            return null;
        }

        @Override
        protected RowRecord peekNextBlobRecord() {
            RowRecord currentRecord = this.currentRecord;
            try {
                this.currentRecord = nextRecord();
            } catch (Exception e) {
                log.error("Can't read record from file " + getCurrentBlob().getBlobPath(), e);
            }

            return currentRecord;
        }

        @Override
        protected void complete() {
            closePreviousIterator();
        }

        private void closePreviousIterator() {
            if (iter != null) {
                try {
                    iter.close();
                } catch (IOException e) {
                    log.error("Can't close stream: {}.", e.getMessage());
                }
            }
        }
    }

}
