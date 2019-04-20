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

package org.talend.components.mongodb.source;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.talend.sdk.component.api.record.Schema.Type.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.mongodb.MongoTestBase;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.utils.MongoDBTestConstants;
import org.talend.components.mongodb.utils.MongoDBTestExtension;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.mongodb")
@ExtendWith(MongoDBTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDBInputMapperTestIT extends MongoTestBase {

    @Injected
    private BaseComponentsHandler componentsHandler;

    private MongoDBTestExtension.TestContext testContext;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private MongoClient client;

    private int documentsCount = 10;

    private Schema schema;

    @BeforeAll
    private void init(MongoDBTestExtension.TestContext testContext) {
        this.testContext = testContext;
    }

    @BeforeEach
    public void prepareData() {
        this.client = createClient(testContext);
        this.schema = createSchema(recordBuilderFactory);
    }

    @Test
    public void testReadFindMapper() throws IOException {
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);
        List<Document> documents = createDocuments(10);

        List<InsertOneModel<Document>> insertModels = documents.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertModels);

        List<Record> testData = convertToRecords(documents.iterator(), schema, recordBuilderFactory);

        MongoDBInputMapperConfiguration configuration = new MongoDBInputMapperConfiguration();
        configuration.setQueryType(MongoDBInputMapperConfiguration.QueryType.FIND_QUERY);
        configuration.setQuery("{}");
        configuration.setSort(Arrays.asList(new Sort("col1", Sort.SortingOrder.desc)));
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setSchema(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        dataset.setCollection(MongoDBTestConstants.COLLECTION_NAME);
        dataset.setDatastore(testContext.getDataStore());
        configuration.setDataset(dataset);

        final String configURI = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("mongoInput", "MongoDB://MongoDBInput?" + configURI).component("collector", "test://collector")
                .connections().from("mongoInput").to("collector").build().run();

        final List<Record> collectedData = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(documentsCount, collectedData.size());

        testData.sort(Comparator.comparingInt(r -> ((Record) r).get(Integer.class, "col1")).reversed());

        assertResultWithOrder(schema, testData, collectedData);
    }

    @Test
    public void testReadAggregationStages() throws IOException {
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);
        List<Document> documents = createDocuments(10);

        List<InsertOneModel<Document>> insertModels = documents.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertModels);

        List<Record> testData = convertToRecords(documents.iterator(), schema, recordBuilderFactory);

        MongoDBInputMapperConfiguration configuration = new MongoDBInputMapperConfiguration();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setSchema(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        dataset.setCollection(MongoDBTestConstants.COLLECTION_NAME);
        dataset.setDatastore(testContext.getDataStore());
        configuration.setDataset(dataset);

        final String configURI = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("mongoInput", "MongoDB://MongoDBInput?" + configURI).component("collector", "test://collector")
                .connections().from("mongoInput").to("collector").build().run();

        final List<Record> collectedData = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(documentsCount, collectedData.size());

        assertResult(schema, testData, collectedData);
    }

    @Test
    public void testReadWithMapping() {
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);
        Document testDocument = Document.parse("{a: 1, b: {c: {f: \"string\", e: 2}}}");
        Document testDocumentWithMappingFlat = Document.parse("{a: 1, f: \"string\", e: 2}");

        List<Document> documents = Arrays.asList(testDocument);

        List<InsertOneModel<Document>> insertModels = documents.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertModels);

        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("a").withType(INT).withNullable(false).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("f").withType(STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("e").withType(INT).build());

        Schema schema = schemaBuilder.build();
        List<Record> testData = convertToRecords(Arrays.asList(testDocumentWithMappingFlat).iterator(), schema,
                recordBuilderFactory);

        MongoDBInputMapperConfiguration configuration = new MongoDBInputMapperConfiguration();
        configuration.setQueryType(MongoDBInputMapperConfiguration.QueryType.FIND_QUERY);
        configuration.setQuery("{}");
        InputMapping mapping = new InputMapping("f", "b.c");
        InputMapping mapping1 = new InputMapping("e", "b.c");
        configuration.setMapping(Arrays.asList(mapping, mapping1));
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setSchema(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        dataset.setCollection(MongoDBTestConstants.COLLECTION_NAME);
        dataset.setDatastore(testContext.getDataStore());
        configuration.setDataset(dataset);

        final String configURI = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("mongoInput", "MongoDB://MongoDBInput?" + configURI).component("collector", "test://collector")
                .connections().from("mongoInput").to("collector").build().run();

        final List<Record> collectedData = componentsHandler.getCollectedData(Record.class);

        Assertions.assertEquals(1, collectedData.size());

        assertResult(schema, testData, collectedData);
    }

    private void assertResultWithOrder(Schema schema, List<Record> testData, List<Record> result) {
        assertEquals(testData.size(), result.size());
        for (int i = 0; i < testData.size(); i++) {
            Record actual = result.get(i);
            Record expected = testData.get(i);
            schema.getEntries().stream().forEach(col -> assertObject(actual, expected, col.getName()));
        }
    }

    @AfterEach
    public void tearDown() {
        client.getDatabase(MongoDBTestConstants.DATABASE_NAME).getCollection(MongoDBTestConstants.COLLECTION_NAME).drop();
    }

    @AfterAll
    public void closeConnection() {
        client.close();
    }

}