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

import com.mongodb.client.model.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.components.mongodb.output.processor.impl.*;
import org.talend.components.mongodb.service.I18nMessage;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

@WithComponents("org.talend.components.mongodb")
public class ModelProducersTest {

    @Service
    private I18nMessage message;

    @Test
    public void testDeleteModelProducer() {
        Document filter = Document.parse("{\"a\":1}");

        DeleteModelProducer producer = new DeleteModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        DeleteOneModel<Document> model = producer.createRecord(message);

        assertEquals(filter, model.getFilter());
    }

    @Test
    public void testInsertModelProducer() {
        Document doc = Document.parse("{\"a\":1, \"b\":2}");

        InsertModelProducer producer = new InsertModelProducer();
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        InsertOneModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getDocument());
    }

    @Test
    public void testSetManyModelProducer() {
        Document doc = new Document("$set", Document.parse("{\"a\":1, \"b\":2}"));
        Document filter = Document.parse("{\"a\":1}");

        SetManyModelProducer producer = new SetManyModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        UpdateManyModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getUpdate());
        assertEquals(filter, model.getFilter());
    }

    @Test
    public void testSetOneModelProducer() {
        Document doc = new Document("$set", Document.parse("{\"a\":1, \"b\":2}"));
        Document filter = Document.parse("{\"a\":1}");

        SetOneModelProducer producer = new SetOneModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        UpdateOneModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getUpdate());
        assertEquals(filter, model.getFilter());
    }

    @Test
    public void testUpdateModelProducer() {
        Document doc = Document.parse("{\"a\":1, \"b\":2}");
        Document filter = Document.parse("{\"a\":1}");

        UpdateModelProducer producer = new UpdateModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        ReplaceOneModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getReplacement());
        assertEquals(filter, model.getFilter());
    }

    @Test
    public void testUpsertModelProducer() {
        Document doc = Document.parse("{\"a\":1, \"b\":2}");
        Document filter = Document.parse("{\"a\":1}");

        UpsertModelProducer producer = new UpsertModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        ReplaceOneModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getReplacement());
        assertEquals(filter, model.getFilter());
        assertEquals(true, model.getReplaceOptions().isUpsert());
    }

    @Test
    public void testUpsertWithSetModelProducer() {
        Document doc = new Document("$set", Document.parse("{\"a\":1, \"b\":2}"));
        Document filter = Document.parse("{\"a\":1}");

        UpsertWithSetModelProducer producer = new UpsertWithSetModelProducer(createConfiguration());
        producer.addField(null, "a", 1);
        producer.addField(null, "b", 2);

        UpdateOneModel<Document> model = producer.createRecord(message);

        assertEquals(doc, model.getUpdate());
        assertEquals(filter, model.getFilter());
        assertEquals(true, model.getOptions().isUpsert());
    }

    private MongoDBOutputConfiguration createConfiguration() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setDatastore(datastore);
        MongoDBOutputConfiguration config = new MongoDBOutputConfiguration();
        config.setDataset(dataset);
        config.setKeys(Arrays.asList("a"));
        return config;
    }

}
