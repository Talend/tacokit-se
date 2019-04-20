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

package org.talend.components.mongodb.service;

import com.mongodb.MongoClient;
import com.mongodb.client.ListDatabasesIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Data;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;
import org.talend.components.mongodb.source.MongoDBInputMapperConfiguration;
import org.talend.components.mongodb.source.Sort;
import org.talend.components.mongodb.utils.MongoDBTestExtension;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.components.mongodb.utils.MongoDBTestConstants.COLLECTION_NAME;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Data
@WithComponents("org.talend.components.mongodb")
@ExtendWith(MongoDBTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class MongoDBServiceTestIT {

    @Service
    private MongoDBService service;

    @Service
    private I18nMessage i18nMessage;

    private MongoDBTestExtension.TestContext testContext;

    @BeforeAll
    private void init(MongoDBTestExtension.TestContext testContext) {
        this.testContext = testContext;
    }

    @Test
    public void testGetClient() {
        MongoClient client = service.getMongoClient(testContext.getDataStore(),
                new DefaultClientOptionsFactory(testContext.getDataStore(), i18nMessage));
        ListDatabasesIterable<Document> databases = client.listDatabases();
        Iterator<Document> iterator = databases.iterator();
        assertTrue(iterator.hasNext());
    }

    @Test
    public void testGetCollection() {
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setCollection(COLLECTION_NAME);
        dataset.setDatastore(testContext.getDataStore());

        MongoClient client = service.getMongoClient(testContext.getDataStore(),
                new DefaultClientOptionsFactory(testContext.getDataStore(), i18nMessage));
        MongoCollection<Document> collection = service.getCollection(dataset, client);
        collection.drop();

        collection.insertOne(Document.parse("{\"a\":1}"));
        assertEquals(1, collection.countDocuments());

        collection.drop();
        client.close();
    }

}
