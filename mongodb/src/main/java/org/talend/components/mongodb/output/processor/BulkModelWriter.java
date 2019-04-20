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

package org.talend.components.mongodb.output.processor;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;

import java.util.ArrayList;
import java.util.List;

public class BulkModelWriter<T extends WriteModel<Document>> implements ModelWriter<T> {

    private final List<T> batch = new ArrayList<>();

    private final MongoCollection<Document> collection;

    private final BulkWriteOptions options;

    public BulkModelWriter(MongoCollection<Document> collection, MongoDBOutputConfiguration.BulkWriteType writeType) {
        this.collection = collection;
        this.options = new BulkWriteOptions().ordered(writeType == MongoDBOutputConfiguration.BulkWriteType.ORDERED);
    }

    @Override
    public void putModel(T model) {
        batch.add(model);
    }

    @Override
    public void flush() {
        if (batch != null && !batch.isEmpty()) {
            collection.bulkWrite(batch, options);
            batch.clear();
        }
    }

    @Override
    public void close() {
        flush();
    }
}
