/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

import com.mongodb.MongoClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.components.mongodb.dataset.MongoDBReadAndWriteDataSet;
import org.talend.components.mongodb.dataset.MongoDBReadDataSet;
import org.talend.components.mongodb.datastore.MongoDBDataStore;
import org.talend.components.mongodb.service.MongoDBService;
import org.talend.components.mongodb.sink.MongoDBSinkConfiguration;
import org.talend.components.mongodb.source.BaseSourceConfiguration;
import org.talend.components.mongodb.source.MongoDBCollectionSourceConfiguration;
import org.talend.components.mongodb.source.MongoDBQuerySourceConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonObject;
import java.util.*;

// TODO current only for local test which have mongo env, will refactor it later
@Disabled
@Slf4j
@WithComponents("org.talend.components.mongodb")
@DisplayName("testing of MongoDB connector")
public class MongoDBTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private MongoDBService mongoDBService;

    @Test
    void testBasic() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("test");

        /*
         * List<PathMapping> pathMappings = new ArrayList<>();
         * pathMappings.add(new PathMapping("_id", "_id", ""));
         * pathMappings.add(new PathMapping("item", "item", ""));
         * pathMappings.add(new PathMapping("qty", "qty", ""));
         * pathMappings.add(new PathMapping("status", "status", ""));
         */

        // dataset.setMode(Mode.MAPPING);
        // dataset.setPathMappings(pathMappings);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
    }

    @Test
    void testDate() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("bakesales");

        /*
         * List<PathMapping> pathMappings = new ArrayList<>();
         * pathMappings.add(new PathMapping("_id", "_id", ""));
         * pathMappings.add(new PathMapping("date", "date", ""));
         * pathMappings.add(new PathMapping("quantity", "quantity", ""));
         * pathMappings.add(new PathMapping("amount", "amount", ""));
         */

        // dataset.setMode(Mode.MAPPING);
        // dataset.setPathMappings(pathMappings);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
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
        MongoDBReadDataSet dataset = getMongoDBDataSet("test");

        dataset.setQuery("{status : \"A\"}");
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
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
        datastore.setAddress(new Address("localhost", 27017));
        datastore.setDatabase("test");
        datastore.setAuth(new Auth());

        MongoDBReadDataSet dataset = new MongoDBReadDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection(collection);
        return dataset;
    }

    private MongoDBReadAndWriteDataSet getMongoDBReadAndWriteDataSet(String collection) {
        MongoDBDataStore datastore = new MongoDBDataStore();
        datastore.setAddress(new Address("localhost", 27017));
        datastore.setDatabase("test");
        datastore.setAuth(new Auth());

        MongoDBReadAndWriteDataSet dataset = new MongoDBReadAndWriteDataSet();
        dataset.setDatastore(datastore);
        dataset.setCollection(collection);
        return dataset;
    }

    @Test
    void testBasicDocumentMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("bakesales");
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        System.out.println(res);
    }

    @Test
    void testHealthCheck() {
        MongoDBDataStore datastore = getMongoDBDataSet("test").getDatastore();
        datastore.setAddress(new Address("localhost", 27017));
        datastore.setDatabase("test1");

        System.out.println(mongoDBService.healthCheck(datastore));
    }

    @Test
    void testMultiServers() {

    }

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
        System.out.println(options.getConnectTimeout());
        System.out.println(options.getApplicationName());

        datastore.setConnectionParameter(Collections.emptyList());
        options = mongoDBService.getOptions(datastore);
        System.out.println(options.getConnectTimeout());
        System.out.println(options.getApplicationName());
    }

    private void executeSourceTestJob(BaseSourceConfiguration configuration) {
        final String sourceConfig = SimpleFactory.configurationByExample().forInstance(configuration).configured()
                .toQueryString();
        Job.components().component("MongoDB_CollectionQuerySource", "MongoDB://CollectionQuerySource?" + sourceConfig)
                .component("collector", "test://collector").connections().from("MongoDB_CollectionQuerySource").to("collector")
                .build().run();
    }

    @Test
    void testSinkBasic() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testSinkBulkWriteAndOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.ORDERED);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testSinkBulkWriteAndUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.UNORDERED);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testUpdate() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("_id", "_id")));

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testUpdateWithBulkWriteOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("_id", "_id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.ORDERED);

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testUpdateWithBulkWriteUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("_id", "_id")));
        config.setBulkWrite(true);
        config.setBulkWriteType(BulkWriteType.UNORDERED);

        componentsHandler.setInputData(getUpdateData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testUpsertWithBulkWriteOrdered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

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
        System.out.println(res);
    }

    @Test
    void testUpsertWithBulkWriteUnordered() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

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
        System.out.println(res);
    }

    @Test
    void testUpsert() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.UPSERT_WITH_SET);
        // i can't use _id here as mongodb upsert limit, TODO show clear exception information
        config.setKeyMappings(Arrays.asList(new KeyMapping("id", "id")));

        componentsHandler.setInputData(getUpsertData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

    @Test
    void testSinkTextMode() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.TEXT);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);

        componentsHandler.setInputData(getTestData4TextMode(getTestData()));
        executeSinkTestJob(config);
    }

    @Test
    void testSinkTextModeUpdate() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.TEXT);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setDataAction(DataAction.SET);
        config.setKeyMappings(Arrays.asList(new KeyMapping("_id", "_id")));

        componentsHandler.setInputData(getTestData4TextMode(getUpdateData()));
        executeSinkTestJob(config);
    }

    @Test
    void testSinkTextModeUpsert() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

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
            MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

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
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withLong("_id", i)
                    .withLong("id", i).withString("name", "wangwei").withInt("score", 100).withDouble("high", 178.5)
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
        for (int i = 2; i < 10; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withLong("_id", i)
                    .withLong("id", i).withString("name", "wangwei1").withInt("score", 100).withDouble("high", 180.5)
                    .withDateTime("birth", new Date()).build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private List<Record> getUpsertData() {
        List<Record> testRecords = new ArrayList<>();
        for (int i = 2; i < 19; i++) {
            Record record = componentsHandler.findService(RecordBuilderFactory.class).newRecordBuilder().withLong("_id", i)
                    .withLong("id", i).withString("name", "wangwei12").withInt("score", 100).withDouble("high", 180.5)
                    .withDateTime("birth", new Date()).build();
            testRecords.add(record);
        }
        return testRecords;
    }

    private void executeSinkTestJob(MongoDBSinkConfiguration configuration) {
        final String sinkConfig = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("emitter", "test://emitter").component("MongoDB_Sink", "MongoDB://Sink?" + sinkConfig)
                .connections().from("emitter").to("MongoDB_Sink").build().run();
    }

    @Test
    void testSpecialWhere() {
        String query = "{$where:function() {\n" + "   return this.item == \"journal\"" + "}}";

        MongoDBReadDataSet dataset = getMongoDBDataSet("inventory");

        dataset.setQuery(query);
        dataset.setMode(Mode.TEXT);

        final List<Record> res = getRecords(dataset);

        System.out.println(res.size());
        System.out.println(res);
    }

    @Test
    void testSourceNullProcess() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("my_collection_01");

        final List<Record> res = getRecords(dataset);

        //"a" exists in schema
        System.out.println(res.get(0).getSchema());
    }

    @Test
    void testDataTypeRoundTrip4TextMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("my_collection_01");
        dataset.setMode(Mode.TEXT);
        final List<Record> res = getRecords(dataset);

        String jsonContent = res.get(0).getString("my_collection_01");
        //the json string should be readable, no too much convert as not only for mongodb, the sink also for other target type like database
        System.out.println(jsonContent);

        //can parse it back with 100% the same
        System.out.println(Document.parse(jsonContent));
    }

    @Test
    void testDataTypeRoundTrip4JsonMode() {
        MongoDBReadDataSet dataset = getMongoDBDataSet("my_collection_01");
        final List<Record> res = getRecords(dataset);

        //the json string should be readable, no too much convert as not only for mongodb, the sink also for other target type like database
        Record record = res.get(0);
        System.out.println(record);

        //should can parse it back with 100% the same
        JsonObject jsonObject = new RecordToJson().fromRecord(record);
        String jsonContent = jsonObject.toString();
        System.out.println(jsonContent);
        Document document = Document.parse(jsonContent);
        System.out.println(document);
    }

    @Test
    void testSpecialDataTypeStructFromPipeLine() {
        //TODO
    }

    @Test
    void testSink_UNACKNOWLEDGED() {
        MongoDBReadAndWriteDataSet dataset = getMongoDBReadAndWriteDataSet("test");

        dataset.setMode(Mode.JSON);

        MongoDBSinkConfiguration config = new MongoDBSinkConfiguration();
        config.setDataset(dataset);
        config.setSetWriteConcern(true);
        config.setWriteConcern(WriteConcern.UNACKNOWLEDGED);

        componentsHandler.setInputData(getTestData());
        executeSinkTestJob(config);

        List<Record> res = getRecords(dataset);
        System.out.println(res);
    }

}
