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
package org.talend.components.bigquery.output;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.talend.sdk.component.api.component.Icon.IconType.BIGQUERY;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@Processor(name = "BigQueryOutput")
@Documentation("This component writes into BigQuery.")
public class BigQueryOutput implements Serializable {

    /** Maximum records per request allowed by Google API */
    private static final int MAX_BATCH_SIZE = 10_000;

    private final I18nMessage i18n;

    private final BigQueryOutputConfig configuration;

    private final BigQueryConnection connection;

    private Schema tableSchema;

    private transient List<Record> records;

    private transient boolean init;

    private transient BigQuery bigQuery;

    private transient TableId tableId;

    private BigQueryService service;

    private JobId jobId;

    private GoogleStorageService storageService;

    private transient Storage storage;

    private RecordIORepository ioRepository;

    private transient RecordWriter recordWriter;

    private transient BlobInfo blobInfo;

    private transient WriteChannel writer;

    private transient boolean isTruncateDone;

    public BigQueryOutput(@Option("configuration") final BigQueryOutputConfig configuration, BigQueryService bigQueryService,
            GoogleStorageService storageService, RecordIORepository ioRepository, I18nMessage i18n) {
        this.configuration = configuration;
        this.connection = configuration.getDataSet().getConnection();
        this.tableSchema = bigQueryService.guessSchema(configuration);
        this.service = bigQueryService;
        this.jobId = getNewUniqueJobId();
        this.storageService = storageService;
        this.ioRepository = ioRepository;
        this.i18n = i18n;
    }

    private JobId getNewUniqueJobId() {
        bigQuery = service.createClient(connection);
        JobId jobId;
        do {
            jobId = JobId.of(UUID.randomUUID().toString());
        } while (bigQuery.getJob(jobId) != null);
        return jobId;
    }

    @PostConstruct
    public void init() {
        bigQuery = service.createClient(connection);
        tableId = TableId.of(connection.getProjectName(), configuration.getDataSet().getBqDataset(),
                configuration.getDataSet().getTableName());
        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {
            storage = storageService.getStorage(bigQuery.getOptions().getCredentials());
        }
    }

    private void truncateTable() {
        Job firstJob = bigQuery.getJob(jobId);
        if (firstJob != null) {
            if (!firstJob.isDone()) {
                try {
                    firstJob.waitFor();
                } catch (InterruptedException e) {
                    throw new BigQueryConnectorException(e.getMessage());
                }
            }
        } else {
            Blob blob = getNewBlob();
            String sourceUri = "gs://" + blob.getBlobId().getBucket() + "/" + blob.getBlobId().getName();
            LoadJobConfiguration loadConfiguration = LoadJobConfiguration.newBuilder(tableId, sourceUri)
                    .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE).build();
            JobInfo jobInfo = JobInfo.newBuilder(loadConfiguration).setJobId(jobId).build();
            Job job = bigQuery.create(jobInfo);
            try {
                job.waitFor();
            } catch (InterruptedException e) {
                throw new BigQueryConnectorException(e.getMessage());
            }
            storage.delete(blob.getBlobId());
            isTruncateDone = true;
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        records = new ArrayList<>();
        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {
            if (!isTruncateDone) {
                truncateTable();
            }
            Blob blob = getNewBlob();
            writer = blob.writer();
            try {
                recordWriter = buildWriter(writer);
            } catch (IOException e) {
                throw new BigQueryConnectorException(e.getMessage());
            }
        }
    }

    private Blob getNewBlob() {
        do {
            String uuid = UUID.randomUUID().toString();
            String blobName = "temp/" + uuid + "/" + configuration.getDataSet().getTableName() + ".csv";
            blobInfo = BlobInfo.newBuilder(configuration.getDataSet().getGsBucket(), blobName).build();
        } while (storage.get(blobInfo.getBlobId()) != null);
        return storage.create(blobInfo);
    }

    private RecordWriter buildWriter(WriteChannel writerChannel) throws IOException {
        final ContentFormat contentFormat = new CSVConfiguration();
        final RecordWriterSupplier recordWriterSupplier = this.ioRepository.findWriter(contentFormat.getClass());
        final RecordWriter writer = recordWriterSupplier.getWriter(() -> Channels.newOutputStream(writerChannel), contentFormat);
        writer.init(contentFormat);
        return writer;
    }

    @ElementListener
    public void onElement(Record record) {
        if (!init) {
            lazyInit();
        }
        records.add(record);
    }

    private void lazyInit() {
        init = true;
        Table table = bigQuery.getTable(tableId);
        if (table != null) {
            tableSchema = table.getDefinition().getSchema();
        } else if (configuration.getTableOperation() != BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            throw new BigQueryConnectorException(i18n.infoTableNoExists(
                    configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
        }
    }

    @AfterGroup
    public void afterGroup() {

        if (!records.isEmpty() && tableSchema == null
                && configuration.getTableOperation() == BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            tableSchema = service.guessSchema(records.get(0));

            if (tableSchema != null) {
                createTableIfNotExist();
            }
            if (tableSchema == null) {
                // Retry reading table metadata, in case another worker created if meanwhile
                Table table = bigQuery.getTable(tableId);
                if (table != null) {
                    tableSchema = table.getDefinition().getSchema();
                }
            }
        }

        if (BigQueryOutputConfig.TableOperation.TRUNCATE == configuration.getTableOperation()) {
            try {
                loadData();
            } catch (BigQueryException e) {
                if (e.getMessage().contains("Already Exists")) {
                    loadData();
                } else {
                    log.warn(e.getMessage());
                }
            }
            storage.delete(blobInfo.getBlobId());
        } else {
            streamData();
        }
    }

    private void createTableIfNotExist() {
        try {
            Table table = bigQuery.getTable(tableId);
            if (table == null) {
                log.info(i18n.infoTableNoExists(configuration.getDataSet().getBqDataset() + "." + tableId.getTable()));
                TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
                bigQuery.create(tableInfo);
                log.info(i18n.infoTableCreated(tableId.getTable()));
            }
        } catch (BigQueryException e) {
            log.warn(i18n.errorCreationTable() + e.getMessage(), e);

        }
    }

    private void streamData() {
        TacoKitRecordToTableRowConverter converter = new TacoKitRecordToTableRowConverter(tableSchema, i18n);

        int nbRecordsToSend = records.size();
        int nbRecordsSent = 0;
        List<Map<String, ?>> recordsBuffer = new ArrayList<>();

        while (nbRecordsSent < nbRecordsToSend) {

            recordsBuffer.clear();
            records.stream().skip(nbRecordsSent).limit(MAX_BATCH_SIZE).map(converter::apply).forEach(recordsBuffer::add);

            InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
            recordsBuffer.stream().forEach(insertAllRequestBuilder::addRow);
            InsertAllResponse response = bigQuery.insertAll(insertAllRequestBuilder.build());

            if (response.hasErrors()) {
                // response.getInsertErrors();
                // rejected no handled by TCK
                log.warn(i18n.warnRejected(response.getInsertErrors().size()));
                // log errors for first row
                response.getInsertErrors().values().iterator().next().stream().forEach(e -> log.warn(e.getMessage()));
                if (response.getInsertErrors().size() == recordsBuffer.size()) {
                    // All rows were rejected : there's an issue with schema ?
                    log.warn(records.get(0).getSchema().toString());
                    log.warn(tableSchema.toString());
                    // Let's show how the first record was handled.
                    log.warn(records.get(nbRecordsSent).toString());
                    log.warn(recordsBuffer.get(0).toString());
                }
            }

            nbRecordsSent += recordsBuffer.size();
        }
    }

    private void loadData() {
        try {
            recordWriter.add(records);
            recordWriter.end();
            writer.close();
        } catch (IOException e) {
            throw new BigQueryConnectorException(e.getMessage());
        }

        JobInfo jobInfo = buildJobInfo();
        Job job = bigQuery.create(jobInfo);
        try {
            job = job.waitFor();
        } catch (InterruptedException e) {
            throw new BigQueryConnectorException(e.getMessage());
        }
        if (job.getStatus().getError() != null) {
            log.warn(i18n.errorBigqueryLoadJob() + job.getStatus().getError());
        }

    }

    private JobInfo buildJobInfo() {
        String sourceUri = "gs://" + blobInfo.getBucket() + "/" + blobInfo.getName();
        LoadJobConfiguration loadConfiguration = LoadJobConfiguration.newBuilder(tableId, sourceUri)
                .setFormatOptions(FormatOptions.csv()).setSchema(tableSchema).build();
        return JobInfo.of(loadConfiguration);
    }

}
