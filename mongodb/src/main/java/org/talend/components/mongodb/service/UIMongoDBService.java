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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UIMongoDBService {

    public static final String HEALTH_CHECK = "healthCheck";

    public static final String GET_SCHEMA_FIELDS = "loadFields";

    @Service
    private MongoDBService mongoDbService;

    @Service
    private I18nMessage i18nMessage;

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus testConnection(MongoDBDatastore datastore) {
        MongoClient mongo = mongoDbService.getMongoClient(datastore, new DefaultClientOptionsFactory(datastore, i18nMessage));
        try {
            MongoDatabase db = mongo.getDatabase(datastore.getDatabase());
            MongoIterable<String> collectionsIterable = db.listCollectionNames();
            Iterator<String> collectionsIterator = collectionsIterable.iterator();
            if (collectionsIterator.hasNext()) {
                System.out.println("getting collection name");
                collectionsIterator.next();
            }
            mongo.close();
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nMessage.connectionFailed(e.getMessage()));
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18nMessage.connectionSuccessful());
    }

    @Suggestions(GET_SCHEMA_FIELDS)
    public SuggestionValues getFields(MongoDBDataset dataset) {
        if (dataset.getSchema() == null || dataset.getSchema().isEmpty()) {
            return new SuggestionValues(false, Collections.emptyList());
        }
        List<SuggestionValues.Item> items = dataset.getSchema().stream().map(s -> new SuggestionValues.Item(s, s))
                .collect(Collectors.toList());
        return new SuggestionValues(false, items);
    }

}
