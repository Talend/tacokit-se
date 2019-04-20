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
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

public abstract class SingleModelWriter<T extends WriteModel<Document>> implements ModelWriter<T> {

    protected final MongoCollection<Document> collection;

    public SingleModelWriter(MongoCollection<Document> collection) {
        this.collection = collection;
    }

    @Override
    public void flush() {
        // noop
    }

    @Override
    public void close() {
        // noop
    }

}
