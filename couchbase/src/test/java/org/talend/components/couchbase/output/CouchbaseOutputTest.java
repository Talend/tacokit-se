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
package org.talend.components.couchbase.output;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseOutput component")
public class CouchbaseOutputTest extends CouchbaseUtilTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    private List<Record> records;

    private List<JsonDocument> retrieveDataFromDatabase() {
        CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC * 1000)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getContainerIpAddress());
        Bucket bucket = cluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);

        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        N1qlQueryResult n1qlQueryResult = bucket.query(N1qlQuery
                .simple("SELECT META(" + BUCKET_NAME + ").id FROM " + BUCKET_NAME + " ORDER BY META(" + BUCKET_NAME + ").id"));
        List<JsonDocument> resultList = n1qlQueryResult.allRows().stream().map(index -> index.value().get("id"))
                .map(Object::toString).map(index -> bucket.get(index)).collect(Collectors.toList());

        bucket.close();
        cluster.disconnect();
        environment.shutdown();
        return resultList;
    }

    @BeforeEach
    void createTestRecords() {
        records = super.createRecords();
        componentsHandler.setInputData(records);
        executeJob();
    }

    void executeJob() {
        final String outputConfig = configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString();

        Job.components().component("Couchbase_Output", "Couchbase://Output?" + outputConfig)
                .component("emitter", "test://emitter").connections().from("emitter").to("Couchbase_Output").build().run();
    }

    @Test
    @DisplayName("Check amount of total records from retrieved data")
    void sizeOfRetrievedCouchbaseInsertTest() {
        assertEquals(2, retrieveDataFromDatabase().size());
    }

    @Test
    @DisplayName("Check fields from retrieved data")
    void checkDataCouchbaseInsertTest() {
        List<JsonDocument> resultList = retrieveDataFromDatabase();
        TestData testData = new TestData();

        assertEquals(new Integer(testData.getCol2()), resultList.get(0).content().getInt("t_int_min"));
        assertEquals(new Integer(testData.getCol3()), resultList.get(0).content().getInt("t_int_max"));
        assertEquals(new Long(testData.getCol4()), resultList.get(0).content().getLong("t_long_min"));
        assertEquals(new Long(testData.getCol5()), resultList.get(0).content().getLong("t_long_max"));
        assertEquals(testData.getCol6(), resultList.get(0).content().getDouble("t_float_min"), 1E35);
        assertEquals(testData.getCol7(), resultList.get(0).content().getDouble("t_float_max"), 1E35);
        assertEquals(testData.getCol8(), resultList.get(0).content().getDouble("t_double_min"), 1);
        assertEquals(testData.getCol9(), resultList.get(0).content().getDouble("t_double_max"), 1);
        assertEquals(testData.isCol10(), resultList.get(0).content().getBoolean("t_boolean"));
        assertEquals(testData.getCol11().toString(), resultList.get(0).content().getString("t_datetime"));
        assertArrayEquals(testData.getCol12().toArray(), resultList.get(0).content().getArray("t_array").toList().toArray());

        assertEquals(2, resultList.size());
    }

    private CouchbaseOutputConfiguration getOutputConfiguration() {
        CouchbaseDataStore couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);

        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setBucket(BUCKET_NAME);
        couchbaseDataSet.setDatastore(couchbaseDataStore);

        CouchbaseOutputConfiguration configuration = new CouchbaseOutputConfiguration();
        configuration.setIdFieldName("t_string");
        configuration.setDataSet(couchbaseDataSet);
        return configuration;
    }
}