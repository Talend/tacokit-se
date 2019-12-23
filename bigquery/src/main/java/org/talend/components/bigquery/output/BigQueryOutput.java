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
package org.talend.components.bigquery.output;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.I18nMessage;
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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    public BigQueryOutput(@Option("configuration") final BigQueryOutputConfig configuration, BigQueryService bigQueryService,
            I18nMessage i18n) {
        this.configuration = configuration;
        this.connection = configuration.getDataSet().getConnection();
        this.tableSchema = bigQueryService.guessSchema(configuration);
        this.service = bigQueryService;
        this.i18n = i18n;
    }

    @PostConstruct
    public void init() {

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
        bigQuery = service.createClient(connection);
        tableId = TableId.of(connection.getProjectName(), configuration.getDataSet().getBqDataset(),
                configuration.getDataSet().getTableName());
        Table table = bigQuery.getTable(tableId);
        if (table != null) {
            tableSchema = table.getDefinition().getSchema();
        } else if (configuration.getTableOperation() != BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            throw new BigQueryConnectorException(i18n.infoTableNoExists(
                    configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        records = new ArrayList<>();
    }

    @AfterGroup
    public void afterGroup() {

        if (!records.isEmpty() && tableSchema == null
                && configuration.getTableOperation() == BigQueryOutputConfig.TableOperation.CREATE_IF_NOT_EXISTS) {
            tableSchema = service.guessSchema(records.get(0));

            if (tableSchema != null) {
                try {
                    Table table = bigQuery.getTable(tableId);
                    if (table == null) {
                        log.info(i18n.infoTableNoExists(
                                configuration.getDataSet().getBqDataset() + "." + configuration.getDataSet().getTableName()));
                        TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(tableSchema)).build();
                        table = bigQuery.create(tableInfo);
                        log.info(i18n.infoTableCreated(tableId.getTable()));
                    }
                } catch (BigQueryException e) {
                    log.warn(i18n.errorCreationTable() + e.getMessage(), e);

                }
            }
            if (tableSchema == null) {
                // Retry reading table metadata, in case another worker created if meanwhile
                Table table = bigQuery.getTable(tableId);
                if (table != null) {
                    tableSchema = table.getDefinition().getSchema();
                }
            }
        }

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

    private TableReference createTableReference() {
        TableReference table = new TableReference();
        table.setProjectId(connection.getProjectName());
        table.setDatasetId(configuration.getDataSet().getBqDataset());
        table.setTableId(configuration.getDataSet().getTableName());
        return table;
    }

}
