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

package org.talend.components.mongodb.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.InsertOneModel;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.mongodb.MongoTestBase;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
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

import java.util.*;
import java.util.stream.Collectors;

@WithComponents("org.talend.components.mongodb")
@ExtendWith(MongoDBTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDBOutputOutputTestIT extends MongoTestBase {

    @Injected
    private BaseComponentsHandler componentsHandler;

    private MongoDBTestExtension.TestContext testContext;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private Schema schema;

    private MongoClient client;

    @BeforeAll
    private void init(MongoDBTestExtension.TestContext testContext) {
        this.testContext = testContext;
    }

    @BeforeEach
    private void setup() {
        this.client = createClient(testContext);
        client.getDatabase(MongoDBTestConstants.DATABASE_NAME).getCollection(MongoDBTestConstants.COLLECTION_NAME).drop();
        schema = createSchema(recordBuilderFactory);
        client.getDatabase(MongoDBTestConstants.DATABASE_NAME).getCollection(MongoDBTestConstants.COLLECTION_NAME).drop();
    }

    @Test
    void insertBulk() {
        final int rowCount = 10;
        List<Record> data = createTestData(rowCount, recordBuilderFactory, schema);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.INSERT);
        outputConfiguration.setDropCollectionIfExists(true);
        outputConfiguration.setBulkWrite(true);
        outputConfiguration.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);
        long documentsCount = collection.countDocuments();

        assertEquals(rowCount, documentsCount);

        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void insertSingle() {
        final int rowCount = 10;
        List<Record> data = createTestData(rowCount, recordBuilderFactory, schema);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.INSERT);
        outputConfiguration.setDropCollectionIfExists(true);
        outputConfiguration.setBulkWrite(false);
        outputConfiguration.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);
        long documentsCount = collection.countDocuments();

        assertEquals(rowCount, documentsCount);

        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void deleteBulk() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        List<Record> data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));

        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.DELETE);
        outputConfiguration.setBulkWrite(true);
        outputConfiguration.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(0, resCount, "Documents have not been deleted");
    }

    @Test
    void deleteSingle() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        List<Record> data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));

        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.DELETE);
        outputConfiguration.setBulkWrite(false);
        outputConfiguration.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(0, resCount, "Documents have not been deleted");
    }

    @Test
    void upsertBulk() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        for (Document doc : testDocs) {
            doc.put("col2", "unknown");
        }

        testDocs.add(createDocument(documentsCount + 1));

        List<Record> data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));

        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT);
        outputConfiguration.setBulkWrite(true);
        outputConfiguration.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount + 1, resCount);

        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void upsertSingle() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        for (Document doc : testDocs) {
            doc.put("col2", "unknown");
        }

        testDocs.add(createDocument(documentsCount + 1));

        List<Record> data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));

        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT);
        outputConfiguration.setBulkWrite(false);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount + 1, resCount);

        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void updateSingle() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        for (Document doc : testDocs) {
            doc.put("col2", "unknown");
        }

        testDocs.add(createDocument(documentsCount + 1));

        List<Record> data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));

        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPDATE);
        outputConfiguration.setBulkWrite(false);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount, resCount);

        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void upsertWithSetMany() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);

        // Set col2 values to unknown, to test if all the rows will be updated with new values.
        for (Document doc : testDocs) {
            doc.put("col2", "unknown");
        }
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        // We will update data in mongo using this document.
        Document updateWithCol2Unknown = createDocument(documentsCount - 1);
        updateWithCol2Unknown.put("col2", "unknown");
        updateWithCol2Unknown.put("col3", 128L);

        List<Record> data = Collections.singletonList(convertToRecord(updateWithCol2Unknown, schema, recordBuilderFactory));
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col2"));
        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT_WITH_SET);
        outputConfiguration.setBulkWrite(false);
        outputConfiguration.setUpdateAllDocuments(true);

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount, resCount);

        for (Document doc : testDocs) {
            doc.put("col3", 128L);
            // Date column. Needs to be updated to be the same as in updated document.
            doc.put("col6", updateWithCol2Unknown.get("col6"));
        }

        data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    void upsertWithSetSingle() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);
        for (Document doc : testDocs) {
            doc.put("col2", "unknown");
        }
        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        Document updateWithCol2Unknown = createDocument(0);
        updateWithCol2Unknown.put("col2", "unknown");
        updateWithCol2Unknown.put("col3", 128L);

        List<Record> data = Collections.singletonList(convertToRecord(updateWithCol2Unknown, schema, recordBuilderFactory));
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col2"));
        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT_WITH_SET);
        outputConfiguration.setBulkWrite(false);
        outputConfiguration.setUpdateAllDocuments(false);

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount, resCount);

        testDocs.stream().filter(c -> c.get("col1").equals(0)).forEach(c -> {
            c.put("col3", 128L);
            c.put("col6", updateWithCol2Unknown.get("col6"));
        });

        data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    @Test
    public void testSet() {
        // Write initial data
        MongoCollection<Document> collection = client.getDatabase(MongoDBTestConstants.DATABASE_NAME)
                .getCollection(MongoDBTestConstants.COLLECTION_NAME);

        int documentsCount = 10;
        List<Document> testDocs = createDocuments(documentsCount);

        List<InsertOneModel<Document>> insertList = testDocs.stream().map(InsertOneModel::new).collect(Collectors.toList());
        collection.bulkWrite(insertList);

        // check that data was inserted
        assertEquals(documentsCount, collection.countDocuments());

        // We will update data in mongo using this document.
        Document updateWithCol2Unknown = createDocument(0);
        updateWithCol2Unknown.put("col2", "unknown");
        updateWithCol2Unknown.put("col3", 128L);

        List<Record> data = Collections.singletonList(convertToRecord(updateWithCol2Unknown, schema, recordBuilderFactory));
        componentsHandler.setInputData(data);

        MongoDBOutputConfiguration outputConfiguration = createConfiguration();
        outputConfiguration.setKeys(Arrays.asList("col1"));
        outputConfiguration.setActionOnData(MongoDBOutputConfiguration.ActionOnData.SET);
        outputConfiguration.setBulkWrite(false);
        outputConfiguration.setUpdateAllDocuments(true);

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();

        Job.components().component("emitter", "test://emitter").component("mongoDBOutput", "MongoDB://MongoDBOutput?" + config)
                .connections().from("emitter").to("mongoDBOutput").build().run();

        long resCount = collection.countDocuments();
        assertEquals(documentsCount, resCount);

        for (Document doc : testDocs) {
            if (doc.get("col1").equals(0)) {
                doc.put("col2", "unknown");
                doc.put("col3", 128L);
                // Date column. Needs to be updated to be the same as in updated document.
                doc.put("col6", updateWithCol2Unknown.get("col6"));
                break;
            }
        }

        data = convertToRecords(testDocs.iterator(), schema, recordBuilderFactory);
        List<Record> result = convertToRecords(collection.find().iterator(), schema, recordBuilderFactory);
        assertResult(schema, data, result);
    }

    private MongoDBOutputConfiguration createConfiguration() {
        MongoDBDatastore datastore = testContext.getDataStore();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setDatastore(datastore);
        dataset.setCollection(MongoDBTestConstants.COLLECTION_NAME);

        MongoDBOutputConfiguration outputConfiguration = new MongoDBOutputConfiguration();
        outputConfiguration.setDataset(dataset);
        return outputConfiguration;
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