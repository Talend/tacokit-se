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
import com.google.auth.Credentials;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import org.apache.avro.file.DataFileStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.bigquery.dataset.TableDataSet;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.GoogleStorageService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class BigQueryTableExtractMapperTest {

    private BigQueryTableExtractInputConfig configuration;

    private BigQueryService service;

    private GoogleStorageService storageService;

    private I18nMessage i18n;

    private RecordBuilderFactory builderFactory;

    private BigQueryConnection connection;

    private BigQuery bigQuery;

    private Storage storage;

    private Table table;

    private Job extractJob;

    private Page<Blob> blobs;

    @BeforeEach
    public void reinit() throws Exception {
        connection = new BigQueryConnection();
        connection.setProjectName("projectName");
        connection.setJsonCredentials("");

        configuration = new BigQueryTableExtractInputConfig();
        TableDataSet tableDataSet = new TableDataSet();
        tableDataSet.setTableName("tableName");
        tableDataSet.setGsBucket("gsBucket");
        tableDataSet.setBqDataset("bqDataset");
        tableDataSet.setConnection(connection);
        configuration.setTableDataset(tableDataSet);

        i18n = Mockito.mock(I18nMessage.class);
        builderFactory = new RecordBuilderFactoryImpl(null);
        service = Mockito.mock(BigQueryService.class);
        storageService = Mockito.mock(GoogleStorageService.class);

        Mockito.doCallRealMethod().when(service).convertToTckField(Mockito.any(FieldValueList.class),
                Mockito.any(Record.Builder.class), Mockito.any(com.google.cloud.bigquery.Field.class), Mockito.any(Schema.class));
        Mockito.doCallRealMethod().when(service).convertToTckSchema(Mockito.any(Schema.class));
        Mockito.doCallRealMethod().when(service).convertToTckType(Mockito.any(LegacySQLTypeName.class),
                Mockito.any(com.google.cloud.bigquery.Field.Mode.class));
        java.lang.reflect.Field rbField = BigQueryService.class.getDeclaredField("recordBuilderFactoryService");
        rbField.setAccessible(true);
        rbField.set(service, builderFactory);

        bigQuery = Mockito.mock(BigQuery.class);
        Mockito.when(service.createClient(connection)).thenReturn(bigQuery);
        BigQueryOptions bqOptions = Mockito.mock(BigQueryOptions.class);
        Mockito.when(bigQuery.getOptions()).thenReturn(bqOptions);
        Credentials credentials = Mockito.mock(Credentials.class);
        Mockito.when(bqOptions.getCredentials()).thenReturn(credentials);

        storage = Mockito.mock(Storage.class);
        Mockito.when(storageService.getStorage(credentials)).thenReturn(storage);
        table = Mockito.mock(Table.class);
        Mockito.when(bigQuery.getTable(Mockito.any(TableId.class))).thenReturn(table);
        extractJob = Mockito.mock(Job.class);
        Mockito.when(bigQuery.create(Mockito.any(JobInfo.class))).thenReturn(extractJob);
        blobs = Mockito.mock(Page.class);
    }

    @Test
    public void testSplit() {
        BigQueryTableExtractMapper beanUnderTest = new BigQueryTableExtractMapper(configuration, service, storageService, i18n,
                builderFactory);

        Mockito.when(table.getNumBytes()).thenReturn(42L);
        TableDefinition td = Mockito.mock(TableDefinition.class);
        Mockito.when(table.getDefinition()).thenReturn(td);
        Schema gSchema = Schema.of(BigQueryQueryInputTest.getFields());
        Mockito.when(td.getSchema()).thenReturn(gSchema);
        Mockito.when(storage.list(Mockito.eq(configuration.getTableDataset().getGsBucket()),
                Mockito.any(Storage.BlobListOption.class))).thenReturn(blobs);

        Iterator<Blob> blobsIterator = Mockito.mock(Iterator.class);
        Iterable<Blob> blobsIterable = new Iterable<Blob>() {

            @Override
            public Iterator<Blob> iterator() {
                return blobsIterator;
            }
        };
        Mockito.when(blobs.iterateAll()).thenReturn(blobsIterable);

        Blob blob1 = Mockito.mock(Blob.class);
        Blob blob2 = Mockito.mock(Blob.class);

        Mockito.when(blobsIterator.hasNext()).thenReturn(true, true, false);
        Mockito.when(blobsIterator.next()).thenReturn(blob1, blob2);

        beanUnderTest.init();
        long estimatedSize = beanUnderTest.estimateSize();
        Assertions.assertEquals(42l, estimatedSize);
        List<BigQueryTableExtractMapper> mappers = beanUnderTest.split(estimatedSize);
        Assertions.assertNotNull(mappers);
        Assertions.assertEquals(2, mappers.size());

        List<BigQueryTableExtractInput> sources = mappers.stream().map(BigQueryTableExtractMapper::createSource)
                .collect(Collectors.toList());

        Assertions.assertNotNull(sources);
        Assertions.assertEquals(2, sources.size());
    }
}
