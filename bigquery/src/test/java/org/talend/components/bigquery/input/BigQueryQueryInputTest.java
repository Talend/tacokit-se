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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.talend.components.bigquery.dataset.QueryDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class BigQueryQueryInputTest {

    private BigQueryQueryInput beanUnderTest;

    private BigQueryQueryInputConfig config;

    private QueryDataSet queryDataSet;

    private BigQueryConnection connection;

    private I18nMessage i18nMessage;

    private RecordBuilderFactory builderFactory;

    private BigQueryService bigQueryService;

    private BigQuery bigQuery;

    private TableResult tableResult;

    private Iterator<FieldValueList> queryResult;

    private Schema tableSchema;

    @BeforeEach
    public void reinit() throws Exception {
        config = new BigQueryQueryInputConfig();
        queryDataSet = new QueryDataSet();
        config.setQueryDataset(queryDataSet);
        connection = new BigQueryConnection();
        queryDataSet.setConnection(connection);
        queryDataSet.setUseLegacySql(true);
        queryDataSet.setQuery("Select * from fake");

        i18nMessage = Mockito.mock(I18nMessage.class);
        builderFactory = new RecordBuilderFactoryImpl(null);
        bigQueryService = Mockito.mock(BigQueryService.class);

        bigQuery = Mockito.mock(BigQuery.class);
        tableResult = Mockito.mock(TableResult.class);

        Mockito.when(bigQueryService.createClient(connection)).thenReturn(bigQuery);
        Mockito.doCallRealMethod().when(bigQueryService).convertToTckField(Mockito.any(FieldValueList.class),
                Mockito.any(Record.Builder.class), Mockito.any(com.google.cloud.bigquery.Field.class), Mockito.any(Schema.class));
        Mockito.doCallRealMethod().when(bigQueryService).convertToTckSchema(Mockito.any(Schema.class));
        Mockito.doCallRealMethod().when(bigQueryService).convertToTckType(Mockito.any(LegacySQLTypeName.class),
                Mockito.any(com.google.cloud.bigquery.Field.Mode.class));
        Field rbField = BigQueryService.class.getDeclaredField("recordBuilderFactoryService");
        rbField.setAccessible(true);
        rbField.set(bigQueryService, builderFactory);

        Mockito.when(bigQuery.query(Mockito.any(QueryJobConfiguration.class))).thenReturn(tableResult);

        queryResult = Mockito.mock(Iterator.class);
        Iterable<FieldValueList> iterable = Mockito.mock(Iterable.class);
        Mockito.when(tableResult.iterateAll()).thenReturn(iterable);
        Mockito.when(iterable.iterator()).thenReturn(queryResult);

        tableSchema = Schema.of(getFields());
        Mockito.when(tableResult.getSchema()).thenReturn(tableSchema);
    }

    @Test
    public void justCrash() {
        try {
            queryDataSet.setQuery(null);
            beanUnderTest = new BigQueryQueryInput(config, bigQueryService, i18nMessage, builderFactory);
            beanUnderTest.init();
            beanUnderTest.next();
            Assertions.fail("Should have thrown a RuntimeException");
        } catch (RuntimeException re) {
            // Exception as expected
        }
    }

    @Test
    public void justRun() throws Exception {
        Mockito.when(queryResult.hasNext()).thenReturn(true, false);
        Mockito.when(queryResult.next()).thenReturn(getRecord(), null);

        beanUnderTest = new BigQueryQueryInput(config, bigQueryService, i18nMessage, builderFactory);
        beanUnderTest.init();

        Record record1 = beanUnderTest.next();
        Field loadedField = BigQueryQueryInput.class.getDeclaredField("loaded");
        loadedField.setAccessible(true);
        boolean loaded = (Boolean) loadedField.get(beanUnderTest);
        Assertions.assertTrue(loaded, "Loading must have occured");
        Assertions.assertNotNull(record1, "Record should not be null");
        // next next() should be null
        Record record2 = beanUnderTest.next();
        Assertions.assertNull(record2, "2nd record should be null");
    }

    public FieldValueList getRecord() {
        List<FieldValue> row = Arrays.asList(new FieldValue[] { FieldValue.of(FieldValue.Attribute.PRIMITIVE, "A"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "0.5"), FieldValue.of(FieldValue.Attribute.PRIMITIVE, "true"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "1978/12/08"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "2019-11-11T09:00:00"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE,
                        Base64.getEncoder().encodeToString("TALEND".getBytes(StandardCharsets.UTF_8))),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "42"),
                FieldValue.of(FieldValue.Attribute.PRIMITIVE, "09:00:00.0100000") });

        FieldList schema = FieldList.of(getFields());
        FieldValueList fvl = FieldValueList.of(row, schema);
        return fvl;
    }

    public static com.google.cloud.bigquery.Field[] getFields() {
        com.google.cloud.bigquery.Field[] fields = new com.google.cloud.bigquery.Field[] {
                com.google.cloud.bigquery.Field.of("f1", LegacySQLTypeName.STRING),
                com.google.cloud.bigquery.Field.of("f2", LegacySQLTypeName.FLOAT),
                com.google.cloud.bigquery.Field.of("f3", LegacySQLTypeName.BOOLEAN),
                com.google.cloud.bigquery.Field.of("f4", LegacySQLTypeName.DATE),
                com.google.cloud.bigquery.Field.of("f5", LegacySQLTypeName.DATETIME),
                com.google.cloud.bigquery.Field.of("f6", LegacySQLTypeName.BYTES),
                com.google.cloud.bigquery.Field.of("f7", LegacySQLTypeName.INTEGER),
                com.google.cloud.bigquery.Field.of("f8", LegacySQLTypeName.TIME) };
        return fields;
    }

}
