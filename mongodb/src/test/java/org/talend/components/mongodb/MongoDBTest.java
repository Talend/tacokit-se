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
package org.talend.components.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.*;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.BigDecimalCodec;
import org.bson.codecs.DocumentCodec;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.rules.TemporaryFolder;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.components.mongodb.dataset.MongoDBReadAndWriteDataSet;
import org.talend.components.mongodb.dataset.MongoDBReadDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.service.MongoDBService;
import org.talend.components.mongodb.service.RecordToDocument;
import org.talend.components.mongodb.sink.MongoDBSinkConfiguration;
import org.talend.components.mongodb.source.BaseSourceConfiguration;
import org.talend.components.mongodb.source.MongoDBCollectionSourceConfiguration;
import org.talend.components.mongodb.source.MongoDBQuerySourceConfiguration;
import org.talend.components.mongodb.source.SplitUtil;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonObject;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@WithComponents("org.talend.components.mongodb")
@DisplayName("testing of MongoDB connector")
public class MongoDBTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private MongoDBService mongoDBService;

    private static final String DATABASE = "testdb";

    private static int port;

    private static MongodExecutable mongodExecutable;

    private static MongodProcess mongodProcess;

    private static MongoClient client;

    public static int getAvailableLocalPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0);) {
            return socket.getLocalPort();
        }
    }

    @BeforeAll
    public static void beforeClass(@TempDir Path tempDir) throws Exception {
        port = getAvailableLocalPort();
        log.info("Starting MongoDB on port {}", port);
        IMongodConfig mongodConfig = new MongodConfigBuilder().version(Version.Main.PRODUCTION).configServer(false)
                .replication(new Storage(tempDir.toFile().getPath(), null, 0))
                .net(new Net("localhost", port, Network.localhostIsIPv6())).cmdOptions(new MongoCmdOptionsBuilder().syncDelay(10)
                        .useNoPrealloc(true).useSmallFiles(true).useNoJournal(true).verbose(false).build())
                .build();
        mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongodConfig);
        mongodProcess = mongodExecutable.start();
        client = new MongoClient("localhost", port);

        MongoDatabase database = client.getDatabase(DATABASE);

        MongoCollection<Document> collection = database.getCollection("basic");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupdate");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupsert");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupdatedifferentpath");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupsertdifferentpath");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupdatebulkordered");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupdatebulkunordered");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupsertbulkordered");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinkupsertbulkunordered");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinktextupdate");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("sinktextupsert");
        collection.insertMany(createTestDocuments());

        collection = database.getCollection("nullprocess");
        Document document = new Document();
        document.put("name", "Wang Wei");
        document.put("address", null);
        collection.insertOne(document);

        collection = database.getCollection("unacknowledged");
        document = new Document();
        document.put("_id", 1);
        document.put("name", "Wang Wei");
        collection.insertOne(document);
    }

    private static List<Document> createTestDocuments() {
        List<Document> result = new ArrayList<>();
        String[] names = new String[] { "Wang Wei", "Xia Liang", "Qi Yan", "Zhao Jin", "Yuan Wei", "Peng Yu", "Xue Jing",
                "Zi Heng", "Wei Zhao", "Pierre" };

        for (int i = 1; i < 11; i++) {
            Document doc = new Document();
            doc.put("id", i);
            doc.put("name", names[i - 1]);
            doc.put("score", 100);
            doc.put("high", 178.5);
            doc.put("birth", new Date());
            doc.put("number", new BigDecimal("123.123"));

            result.add(doc);
        }

        return result;
    }

    @AfterAll
    public static void afterClass() {
        log.info("Stopping MongoDB");
        client.close();
        mongodProcess.stop();
        mongodExecutable.stop();
    }

    @Test
    void testBasic() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");

        final List<Record> res = getRecords(dataset);

        Assertions.assertEquals(10, res.size());

        Record record = res.get(0);

        List<Schema.Entry> entries = record.getSchema().getEntries();
        Assertions.assertEquals("_id", entries.get(0).getName());
        Assertions.assertEquals(Schema.Type.STRING, entries.get(0).getType());

        Assertions.assertEquals("id", entries.get(1).getName());
        Assertions.assertEquals(Schema.Type.INT, entries.get(1).getType());

        Assertions.assertEquals("name", entries.get(2).getName());
        Assertions.assertEquals(Schema.Type.STRING, entries.get(2).getType());

        Assertions.assertEquals("score", entries.get(3).getName());
        Assertions.assertEquals(Schema.Type.INT, entries.get(3).getType());

        Assertions.assertEquals("high", entries.get(4).getName());
        Assertions.assertEquals(Schema.Type.DOUBLE, entries.get(4).getType());

        Assertions.assertEquals("birth", entries.get(5).getName());
        Assertions.assertEquals(Schema.Type.DATETIME, entries.get(5).getType());

        Assertions.assertEquals("number", entries.get(6).getName());
        Assertions.assertEquals(Schema.Type.STRING, entries.get(6).getType());

        Assertions.assertTrue(record.get(Object.class, "_id") instanceof String);
        Assertions.assertEquals(1, record.get(Object.class, "id"));
        Assertions.assertEquals("Wang Wei", record.get(Object.class, "name"));
        Assertions.assertEquals(100, record.get(Object.class, "score"));
        Assertions.assertEquals(178.5, record.get(Object.class, "high"));
        Assertions.assertTrue(record.get(Object.class, "birth") instanceof Long);
        Assertions.assertEquals("123.123", record.get(Object.class, "number"));
    }

    private List<Record> getRecords(MongoDBReadDataSet dataset) {
        MongoDBQuerySourceConfiguration config = new MongoDBQuerySourceConfiguration();
        config.setDataset(dataset);

        executeSourceTestJob(config);
        return componentsHandler.getCollectedData(Record.class);
    }

    private List<Record> getRecords(MongoDBReadAndWriteDataSet dataset) {
        MongoDBCollectionSourceConfiguration config = new MongoDBCollectionSourceConfiguration();
        config.setDataset(dataset);

        executeSourceTestJob(config);
        return componentsHandler.getCollectedData(Record.class);
    }

    @Test
    void testFind() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");

        dataset.setQuery("{name : \"Wang Wei\"}");
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        Assertions.assertEquals(1, res.size());
    }

    /*
     * @Test
     * void testAggregation() {
     * MongoDBReadDataSet dataset = getMongoDBDataSet("bakesales");
     * 
     * List<PathMapping> pathMappings = new ArrayList<>();
     * pathMappings.add(new PathMapping("_id", "_id", ""));
     * pathMappings.add(new PathMapping("sales_quantity", "sales_quantity", ""));
     * pathMappings.add(new PathMapping("sales_amount", "sales_amount", ""));
     * 
     * dataset.setPathMappings(pathMappings);
     * 
     * List<AggregationStage> stages = new ArrayList<>();
     * AggregationStage stage = new AggregationStage();
     * stage.setStage("{ $match: { date: { $gte: new ISODate(\"2018-12-05\") } } }");
     * stages.add(stage);
     * 
     * stage = new AggregationStage();
     * stage.setStage(
     * "{ $group: { _id: { $dateToString: { format: \"%Y-%m\", date: \"$date\" } }, sales_quantity: { $sum: \"$quantity\"}, sales_amount: { $sum: \"$amount\" } } }"
     * );
     * stages.add(stage);
     * 
     * dataset.setQueryType(QueryType.AGGREGATION);
     * dataset.setAggregationStages(stages);
     * 
     * MongoDBQuerySourceConfiguration config = new MongoDBQuerySourceConfiguration();
     * config.setDataset(dataset);
     * 
     * executeJob(config);
     * final List<Record> res = componentsHandler.getCollectedData(Record.class);
     * 
     * System.out.println(res);
     * }
     */

    private MongoDBReadDataSet getMongoDBDataSet(String collection) {
        MongoDBDataStore datastore = new MongoDBDataStore();
        datastore.setAddress(new Address("localhost", port));
        datastore.setDatabase(DATABASE);
        datastore.setAuth(new Auth());

        MongoDBReadDataSet dataset = new MongoDBReadDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection(collection);
        return dataset;
    }

    private MongoDBReadAndWriteDataSet getMongoDBReadAndWriteDataSet(String collection) {
        MongoDBDataStore datastore = new MongoDBDataStore();
        datastore.setAddress(new Address("localhost", port));
        datastore.setDatabase(DATABASE);
        datastore.setAuth(new Auth());

        MongoDBReadAndWriteDataSet dataset = new MongoDBReadAndWriteDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection(collection);
        return dataset;
    }

    @Test
    void testBasicDocumentMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testHealthCheck() {
        MongoDBDataStore datastore = getMongoDBDataSet("basic").getDatastore();
        datastore.setAddress(new Address("localhost", port));
        datastore.setDatabase(DATABASE);

        Assertions.assertEquals(HealthCheckStatus.Status.OK, mongoDBService.healthCheck(datastore).getStatus());
    }

    @Disabled
    @Test
    void testMultiServers() {
        MongoDBDataStore datastore = new MongoDBDataStore();
        datastore.setAddressType(AddressType.REPLICA_SET);
        datastore.setReplicaSetAddress(Arrays.asList(new Address("192.168.31.228", 27017), new Address("192.168.31.228", 27018),
                new Address("192.168.31.228", 27019)));
        datastore.setDatabase("admin");
        datastore.setAuth(new Auth());

        HealthCheckStatus status = mongoDBService.healthCheck(datastore);

        MongoDBReadAndWriteDataSet dataset = new MongoDBReadAndWriteDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection("test123");

        // MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        // config.setDataset(dataset);
        //
        // componentsHandler.setInputData(getTestData());
        // executeSinkTestJob(config);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
    }

    @Disabled
    @Test
    void testAuth() {

    }

    @Test
    void testGetOptions() {
        MongoDBDataStore datastore = new MongoDBDataStore();
        List<ConnectionParameter> cp = Arrays.asList(new ConnectionParameter("connectTimeoutMS", "300000"),
                new ConnectionParameter("appName", "myapp"));
        datastore.setConnectionParameter(cp);
        MongoClientOptions options = mongoDBService.getOptions(datastore);
        Assertions.assertEquals(300000, options.getConnectTimeout());
        Assertions.assertEquals("myapp", options.getApplicationName());

        datastore.setConnectionParameter(Collections.emptyList());
        options = mongoDBService.getOptions(datastore);
        Assertions.assertNull(options.getApplicationName());
    }

    private void executeSourceTestJob(BaseSourceConfiguration configuration) {
        String sourceConfig = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        if (configuration.getSampleLimit() != null) {
            sourceConfig = sourceConfig + "&configuration.sampleLimit=" + configuration.getSampleLimit();
        }

        Job.components().component("MongoDB_CollectionQuerySource", "MongoDB://CollectionQuerySource?" + sourceConfig)
                .component("collector", "test://collector").connections().from("MongoDB_CollectionQuerySource").to("collector")
                .build().run();
    }

    @Test
    void testSinkBasic() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sink");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testSinkBulkWriteAndOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkbulkordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.ORDERED);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testSinkBulkWriteAndUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkbulkunordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.UNORDERED);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testUpdate() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupdate");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testUpdateWithDifferentPath() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupdatedifferentpath");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("myid", "id")));

        componentsHandler.setInputData(getUpdateDataForDifferentPath());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testUpsertWithDifferentPath() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupsertdifferentpath");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("myid", "id")));

        componentsHandler.setInputData(getUpsertDataForDifferentPath());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(20, res.size());
    }

    @Test
    void testUpdateWithBulkWriteOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupdatebulkordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.ORDERED);

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testUpdateWithBulkWriteUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupdatebulkunordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.UNORDERED);

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

    @Test
    void testUpsertWithBulkWriteOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupsertbulkordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.ORDERED);

        componentsHandler.setInputData(getUpsertData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(20, res.size());
    }

    @Test
    void testUpsertWithBulkWriteUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupsertbulkunordered");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.UNORDERED);

        componentsHandler.setInputData(getUpsertData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(20, res.size());
    }

    @Test
    void testUpsert() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinkupsert");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        // i can't use _id here as mongodb upsert limit, TODO show clear exception information
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));

        componentsHandler.setInputData(getUpsertData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(20, res.size());
    }

    @Test
    void testSinkTextMode() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinktext");

        dataset.setMode(Mode.TEXT);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);

        componentsHandler.setInputData(getTestData4TextMode(getTestData()));
        executeSinkTestJob(config);
    }

    @Test
    void testSinkTextModeUpdate() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinktextupdate");

        dataset.setMode(Mode.TEXT);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));

        componentsHandler.setInputData(getTestData4TextMode(getUpdateData()));
        executeSinkTestJob(config);
    }

    @Test
    void testSinkTextModeUpsert() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinktextupsert");

        dataset.setMode(Mode.TEXT);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));

        componentsHandler.setInputData(getTestData4TextMode(getUpsertData()));
        executeSinkTestJob(config);
    }

    @Test
    void testSinkTextModeWithWrongInput() {
        Assertions.assertThrows(RuntimeException.class, () -> {
            MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("sinktextwronginput");

            dataset.setMode(Mode.TEXT);

            MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
            config.setDataset(dataset);

            componentsHandler.setInputData(getTestData());
            executeSinkTestJob(config);
        });
    }

    private List<Record> getTestData() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("id", i)
                    .withString("name", "God" + i).withInt("score", 100).withDouble("high", 178.5)
                    .withDateTime("birth", new Date()).build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getDuplicatedKeyTestData() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("_id", i)
                    .withString("name", "God" + i).withInt("score", 100).withDouble("high", 178.5)
                    .withDateTime("birth", new Date()).build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getTestData4TextMode(List<Record> records) {
        RecordToJson rtj = new RecordToJson();
        List<Record> testRecords = new ArrayList<>();
        for (Record record : records) {
            Record r = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder()
                    .withString("content", rtj.fromRecord(record).toString()).build();
            testRecords.add(r);
        }
        return testRecords;
    }

    private List<Record> getUpdateData() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("id", i)
                    .withString("name", "God").withInt("score", 100).withDouble("high", 180.5).withDateTime("birth", new Date())
                    .build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getUpdateDataForDifferentPath() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 11; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("myid", i)
                    .withString("name", "God").withInt("score", 100).withDouble("high", 180.5).withDateTime("birth", new Date())
                    .build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getUpsertDataForDifferentPath() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 21; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("myid", i)
                    .withString("name", "God").withInt("score", 100).withDouble("high", 180.5).withDateTime("birth", new Date())
                    .build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getUpsertData() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 1; i < 21; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withInt("id", i)
                    .withString("name", "God").withInt("score", 100).withDouble("high", 180.5).withDateTime("birth", new Date())
                    .build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private void executeSinkTestJob(MongoDBSinkConfiguration configuration) {
        final String sinkConfig = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("emitter", "test://emitter").component("MongoDB_Sink", "MongoDB://Sink?" + sinkConfig)
                .connections().from("emitter").to("MongoDB_Sink").build().run();
    }

    private void executeSourceAndSinkTestJob(BaseSourceConfiguration source_config, MongoDBSinkConfiguration sink_config) {
        String sourceConfig = SimpleFactory.configurationByExample().forInstance(source_config).configured().toQueryString();

        if (source_config.getSampleLimit() != null) {
            sourceConfig = sourceConfig + "&configuration.sampleLimit=" + source_config.getSampleLimit();
        }

        final String sinkConfig = SimpleFactory.configurationByExample().forInstance(sink_config).configured().toQueryString();

        Job.components().component("MongoDB_CollectionQuerySource", "MongoDB://CollectionQuerySource?" + sourceConfig)
                .component("MongoDB_Sink", "MongoDB://Sink?" + sinkConfig).connections().from("MongoDB_CollectionQuerySource")
                .to("MongoDB_Sink").build().run();
    }

    @Test
    void testSpecialWhere() {
        String query = "{$where:function() {\n" + "   return this.name == \"Wang Wei\"" + "}}";

        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");

        dataset.setQuery(query);
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        Assertions.assertEquals(1, res.size());
    }

    @Test
    void testSourceNullProcess() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("nullprocess");

        final List<Record> res = getRecords(dataset);

        Assertions.assertEquals(3, res.get(0).getSchema().getEntries().size());
    }

    @Test
    void testDataTypeRoundTrip4TextMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");
        dataset.setMode(Mode.TEXT);
        final List<Record> res = getRecords(dataset);

        String jsonContent = res.get(0).getString("basic");

        Document document = Document.parse(jsonContent);

        Document expected = client.getDatabase(DATABASE).getCollection("basic").find().iterator().next();
        Assertions.assertEquals(expected, document);
    }

    @Test
    void testDataTypeRoundTrip4JsonMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("basic");
        final List<Record> res = getRecords(dataset);

        // the json string should be readable, no too much convert as not only for mongodb, the sink also for other target type
        // like database
        Record record = res.get(0);

        // should can parse it back with 100% the same
        Document document = new RecordToDocument().fromRecord(record);

        Document expected = client.getDatabase(DATABASE).getCollection("basic").find().iterator().next();
        Assertions.assertEquals(expected, document);
    }

    @Test
    void testDataTypeRoundTrip4JsonMode2() {
        MongoDBReadDataSet source_dataset = getMongoDBDataSet("basic");
        source_dataset.setMode(Mode.JSON);
        MongoDBQuerySourceConfiguration source_config = new MongoDBQuerySourceConfiguration();
        source_config.setDataset(source_dataset);

        MongoDBReadAndWriteDataSet sink_dataset = getMongoDBReadAndWriteDataSet("sinktarget");
        sink_dataset.setMode(Mode.JSON);
        MongoDBSinkConfiguration sink_config = new MongoDBSinkConfiguration();
        sink_config.setDataset(sink_dataset);

        // the json string should be readable, no too much convert as not only for mongodb, the sink also for other target type
        // like database
        executeSourceAndSinkTestJob(source_config, sink_config);

        // TODO do check more
    }

    @Test
    void testSpecialDataTypeStructFromPipeLine() {
        // TODO
    }

    @Test
    void testSplit() {
        MongoDBReadDataSet source_dataset = getMongoDBDataSet("basic");
        source_dataset.setMode(Mode.JSON);
        MongoDBQuerySourceConfiguration source_config = new MongoDBQuerySourceConfiguration();
        source_config.setDataset(source_dataset);

        Assertions.assertFalse(SplitUtil.isSplit("{ }", null));
        Assertions.assertFalse(SplitUtil.isSplit(null, 8l));
        Assertions.assertTrue(SplitUtil.isSplit(null, null));
        Assertions.assertTrue(SplitUtil.isSplit(null, -1l));
        Assertions.assertFalse(SplitUtil.isSplit(8l));
        Assertions.assertTrue(SplitUtil.isSplit(null));
        Assertions.assertTrue(SplitUtil.isSplit(-1l));
        List<String> result = SplitUtil.getQueries4Split(source_config, new MongoDBService(), 5);
        Assertions.assertEquals(5, result.size());
    }

    @Disabled
    @Test
    void testSplit2() {
        MongoDBDataStore datastore = new MongoDBDataStore();
        datastore.setAddress(new Address("192.168.31.228", 27017));
        datastore.setDatabase("test");
        Auth auth = new Auth();
        auth.setNeedAuth(true);
        auth.setUsername("");
        auth.setPassword("");
        datastore.setAuth(auth);

        MongoDBReadDataSet dataset = new MongoDBReadDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection("my_collection_01");

        dataset.setMode(Mode.JSON);
        MongoDBQuerySourceConfiguration source_config = new MongoDBQuerySourceConfiguration();
        source_config.setDataset(dataset);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
        /*
         * List<String> result = SplitUtil.getQueries4Split(source_config, new MongoDBService(), 5);
         * System.out.println(result);
         * result.stream().forEach(query -> {
         * BsonDocument doc = new MongoDBService().getBsonDocument(query);
         * System.out.println(doc);
         * });
         */
    }

    @Test
    void testSink_UNACKNOWLEDGED() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("unacknowledged");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setSetWriteConcern(true);
        config.setWriteConcern(WriteConcern.UNACKNOWLEDGED);

        componentsHandler.setInputData(getDuplicatedKeyTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        Assertions.assertEquals(10, res.size());
    }

}
