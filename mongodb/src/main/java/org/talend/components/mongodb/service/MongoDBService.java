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

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;
import org.talend.components.mongodb.source.MongoDBInputMapperConfiguration;
import org.talend.sdk.component.api.service.Service;

@Service
@Slf4j
public class MongoDBService {

    @Service
    private I18nMessage i18nMessage;

    public MongoClient getMongoClient(final MongoDBDatastore datastore, final ClientOptionsFactory optionsFactory) {
        MongoClientFactory factory = MongoClientFactory.getInstance(datastore, optionsFactory.createOptions(), i18nMessage);
        log.debug(i18nMessage.factoryClass(factory.getClass().getName()));
        MongoClient mongo = factory.createClient();
        return mongo;
    }

    public MongoCollection<Document> getCollection(final MongoDBDataset dataset, final MongoClient client) {
        log.debug(i18nMessage.retrievingCollection(dataset.getCollection()));
        MongoDatabase db = client.getDatabase(dataset.getDatastore().getDatabase());
        return db.getCollection(dataset.getCollection());
    }

}