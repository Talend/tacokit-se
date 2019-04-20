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

import com.mongodb.client.model.InsertOneModel;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.talend.components.mongodb.output.MongoDocumentWrapper;
import org.talend.components.mongodb.output.OutputMapping;
import org.talend.components.mongodb.output.processor.ModelProducer;
import org.talend.components.mongodb.service.I18nMessage;

@Slf4j
public class InsertModelProducer implements ModelProducer<InsertOneModel<Document>> {

    private MongoDocumentWrapper mongoDocumentWrapper;

    @Override
    public void addField(OutputMapping mapping, String col, Object value) {
        if (mongoDocumentWrapper == null) {
            mongoDocumentWrapper = new MongoDocumentWrapper();
        }
        mongoDocumentWrapper.put(mapping, col, value);
    }

    @Override
    public InsertOneModel<Document> createRecord(I18nMessage i18nMessage) {
        InsertOneModel<Document> model = new InsertOneModel<>(mongoDocumentWrapper.getObject());
        mongoDocumentWrapper = null;
        return model;
    }
}
