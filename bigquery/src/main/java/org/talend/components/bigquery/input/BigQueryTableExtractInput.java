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
package org.talend.components.bigquery.input;

import com.google.api.gax.paging.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.talend.components.bigquery.avro.AvroConverter;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.Iterator;

@Slf4j
public class BigQueryTableExtractInput implements Serializable {

    protected final BigQueryConnection connection;

    protected final BigQueryService service;

    protected final GoogleStorageService storageService;

    protected final I18nMessage i18n;

    protected final RecordBuilderFactory builderFactory;

    protected final String bucket;

    protected final String gsBlob;

    private final Schema tckSchema;

    private transient Storage storage;

    private transient DataFileStream<GenericRecord> dataStream;

    private transient boolean loaded = false;

    private transient BigQueryTableInput delegateInput;

    private transient AvroConverter converter;

    public BigQueryTableExtractInput(BigQueryTableExtractInputConfig configuration, final BigQueryService service,
            final GoogleStorageService storageService, final I18nMessage i18n, final RecordBuilderFactory builderFactory,
            final String gsBlob, Schema tckSchema) {
        this.bucket = configuration.getTableDataset().getGsBucket();
        this.connection = configuration.getDataStore();
        this.service = service;
        this.storageService = storageService;
        this.i18n = i18n;
        this.builderFactory = builderFactory;
        this.gsBlob = gsBlob;
        this.tckSchema = tckSchema;

        if (gsBlob == null) {
            // Call from Data inventory for a sample : use BigQueryTableInput
            // Remove this whenever the sampling mechanism changes to something better...
            BigQueryTableInputConfig delegateCfg = new BigQueryTableInputConfig();
            delegateCfg.setTableDataset(configuration.getTableDataset());
            delegateInput = new BigQueryTableInput(delegateCfg, service, i18n, builderFactory);
        }
    }

    @PostConstruct
    public void init() {

    }

    @Producer
    public Record next() {
        if (delegateInput != null) {
            return delegateInput.next();
        }

        if (!loaded) {
            try {
                BigQuery bigQuery = service.createClient(connection);

                converter = AvroConverter.of(builderFactory, tckSchema);
                storage = storageService.getStorage(bigQuery.getOptions().getCredentials());
                dataStream = storageService.getDataFileStream(storage, bucket, gsBlob);
            } catch (Exception e) {
                log.error(i18n.errorBlobReaderInit(), e);
                throw new BigQueryConnectorException(e);
            } finally {
                loaded = true;
            }
        }

        Record record = null;

        if (dataStream != null && !dataStream.hasNext()) {
            try {
                dataStream.close();
            } catch (Exception e) {
                log.warn("Cannot close stream", e);
            }
        } else if (dataStream != null) {
            GenericRecord rec = dataStream.next();
            record = converter.toRecord(rec);
        }

        return record;
    }

    @PreDestroy
    public void release() {
        if (gsBlob != null) {
            storageService.deleteBlob(storage, bucket, gsBlob);
        }
    }

}
