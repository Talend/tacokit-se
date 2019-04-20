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

package org.talend.components.mongodb.output.processor.impl;

import com.mongodb.client.model.UpdateOneModel;
import org.bson.Document;
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;

public class SetOneModelProducer extends AbstractFilterValueModelProducer<UpdateOneModel<Document>> {

    public SetOneModelProducer(MongoDBOutputConfiguration configuration) {
        super(configuration);
    }

    @Override
    protected UpdateOneModel<Document> doCreateModel(Document filter, Document object) {
        return new UpdateOneModel<>(filter, new Document("$set", object));
    }
}
