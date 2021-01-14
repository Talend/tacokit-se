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

import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryConnectorException;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "bigquery")
@Documentation("This component reads a table from BigQuery.")
@Slf4j
public class BigQueryTableInput implements Serializable {

    protected final BigQueryConnection connection;

    protected final BigQueryService service;

    protected final I18nMessage i18n;

    protected final RecordBuilderFactory builderFactory;

    private TableDataSet dataSet;

    private transient Schema tableSchema;

    private transient org.talend.sdk.component.api.record.Schema tckSchema;

    private transient Iterator<FieldValueList> queryResult;

    private transient boolean loaded = false;

    public BigQueryTableInput(@Option("configuration") BigQueryTableInputConfig configuration, final BigQueryService service,
            final I18nMessage i18n, final RecordBuilderFactory builderFactory) {
        this.connection = configuration.getDataStore();
        this.service = service;
        this.i18n = i18n;
        this.builderFactory = builderFactory;
        this.dataSet = configuration.getTableDataset();
    }

    @PostConstruct
    public void init() {

    }

    @Producer
    public Record next() {
        if (!loaded) {
            try {
                BigQuery bigQuery = service.createClient(connection);
                TableId tableId = TableId.of(connection.getProjectName(), dataSet.getBqDataset(), dataSet.getTableName());
                Table table = bigQuery.getTable(tableId);
                if (table == null) {
                    throw new BigQueryConnectorException(
                            i18n.infoTableNoExists(dataSet.getBqDataset() + "." + dataSet.getTableName()));
                }
                tableSchema = table.getDefinition().getSchema();

                tckSchema = service.convertToTckSchema(tableSchema);

                TableResult tableResult = bigQuery.listTableData(tableId, tableSchema);
                queryResult = tableResult.iterateAll().iterator();
            } catch (Exception e) {
                log.error(i18n.errorQueryExecution(), e);
            } finally {
                loaded = true;
            }
        }

        Record record = null;

        if (queryResult != null && queryResult.hasNext()) {
            FieldValueList fieldValueList = queryResult.next();

            Record.Builder rb = builderFactory.newRecordBuilder(tckSchema);

            for (Field f : tableSchema.getFields()) {
                service.convertToTckField(fieldValueList, rb, f, tableSchema);

            }

            record = rb.build();
        }

        return record;
    }

}
