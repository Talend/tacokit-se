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
package org.talend.components.couchbase.source;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import org.junit.jupiter.api.*;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseInput component")
public class CouchbaseInputTest extends CouchbaseUtilTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private void insertTestDataToDB() {
        CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC * 1000)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getContainerIpAddress());
        Bucket bucket = cluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);

        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        bucket.bucketManager().flush();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<JsonObject> jsonObjects = super.createJsonObjects();

        bucket.insert(JsonDocument.create("RRRR1", jsonObjects.get(0)));
        bucket.insert(JsonDocument.create("RRRR2", jsonObjects.get(1)));

        // Wait while data is writing (Jenkins fix)
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        bucket.close();
        cluster.disconnect();
        environment.shutdown();
    }

    private void insertTestDataWithNullValueToDB() {
        CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC * 1000)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getContainerIpAddress());
        Bucket bucket = cluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);

        bucket.bucketManager().flush();

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        JsonObject json = JsonObject.create().put("t_string1", "RRRR1").put("t_string2", "RRRR2").putNull("t_string3");

        bucket.insert(JsonDocument.create("RRRR1", json));

        // Wait while data is writing (Jenkins fix)
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        bucket.close();
        cluster.disconnect();
        environment.shutdown();
    }

    void executeJob(CouchbaseInputConfiguration configuration) {
        final String inputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("Couchbase_Input", "Couchbase://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("Couchbase_Input").to("collector").build().run();
    }

    @Test
    @DisplayName("Check input data")
    void couchbaseInputDataTest() {
        componentsHandler.resetState();
        insertTestDataToDB();

        executeJob(getInputConfiguration());

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        TestData testData = new TestData();

        assertNotNull(res);

        assertEquals(2, res.size());

        assertEquals(testData.getCol1() + "1", res.get(0).getString("t_string"));
        assertEquals(testData.getCol2(), res.get(0).getInt("t_int_min"));
        assertEquals(testData.getCol3(), res.get(0).getInt("t_int_max"));
        assertEquals(testData.getCol4(), res.get(0).getLong("t_long_min"));
        assertEquals(testData.getCol5(), res.get(0).getLong("t_long_max"));
        assertEquals(testData.getCol6(), res.get(0).getFloat("t_float_min"));
        assertEquals(testData.getCol7(), res.get(0).getFloat("t_float_max"));
        assertEquals(testData.getCol8(), res.get(0).getDouble("t_double_min"));
        assertEquals(testData.getCol9(), res.get(0).getDouble("t_double_max"));
        assertEquals(testData.isCol10(), res.get(0).getBoolean("t_boolean"));
        assertEquals(testData.getCol11().toString(), res.get(0).getDateTime("t_datetime").toString());
        // assertEquals(testData.getCol12(), res.get(0).getArray(List.class, "t_array"));

        assertEquals(testData.getCol1() + "2", res.get(1).getString("t_string"));
    }

    @Test
    @DisplayName("When input data is null, record will be skipped")
    void firstValueIsNullInInputDBTest() {
        insertTestDataWithNullValueToDB();

        executeJob(getInputConfiguration());

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);

        assertFalse(res.isEmpty());

        assertEquals(2, res.get(0).getSchema().getEntries().size());
    }

    @Test
    @DisplayName("Execution of customN1QL query")
    void n1qlQueryInputDBTest() {
        insertTestDataToDB();

        CouchbaseInputConfiguration configurationWithN1ql = getInputConfiguration();
        configurationWithN1ql.setUseN1QLQuery(true);
        configurationWithN1ql.setQuery("SELECT `t_long_max`, `t_string`, `t_double_max` FROM " + BUCKET_NAME);
        executeJob(configurationWithN1ql);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertEquals(2, res.size());
        assertEquals(3, res.get(0).getSchema().getEntries().size());
        assertEquals(3, res.get(1).getSchema().getEntries().size());
    }

    private CouchbaseInputConfiguration getInputConfiguration() {
        CouchbaseDataStore couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);

        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setDatastore(couchbaseDataStore);
        couchbaseDataSet.setBucket(BUCKET_NAME);

        CouchbaseInputConfiguration configuration = new CouchbaseInputConfiguration();
        return configuration.setDataSet(couchbaseDataSet);
    }
}
