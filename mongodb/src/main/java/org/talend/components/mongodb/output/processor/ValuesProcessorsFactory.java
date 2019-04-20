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
import com.mongodb.client.model.*;
import org.bson.Document;
import org.talend.components.mongodb.output.MongoDBOutputConfiguration;
import org.talend.components.mongodb.output.processor.impl.*;
import org.talend.components.mongodb.service.I18nMessage;

public class ValuesProcessorsFactory {

    public static <T extends WriteModel<Document>> ValuesProcessor<T> createProcessor(MongoDBOutputConfiguration config,
            MongoCollection<Document> collection, I18nMessage i18nMessage) {
        AbstractValuesProcessorFactory<T> factory = createAbstractValuesProcessorFactory(config);
        ModelWriter<T> writer;
        if (config.isBulkWrite()) {
            writer = new BulkModelWriter<>(collection, config.getBulkWriteType());
        } else {
            writer = factory.createWriter(collection);
        }
        ValuesProcessor<T> processor = new ValuesProcessor<>(factory.createProducer(config), writer, i18nMessage);
        return processor;
    }

    public static <T extends WriteModel<Document>> AbstractValuesProcessorFactory<T> createAbstractValuesProcessorFactory(
            MongoDBOutputConfiguration config) {
        AbstractValuesProcessorFactory<T> factory;
        switch (config.getActionOnData()) {
        case INSERT:
            factory = (AbstractValuesProcessorFactory<T>) new InsertModelProcessorFactory();
            break;
        case DELETE:
            factory = (AbstractValuesProcessorFactory<T>) new DeleteModelProcessorFactory();
            break;
        case UPDATE:
            factory = (AbstractValuesProcessorFactory<T>) new UpdateModelProcessFactory();
            break;
        case SET:
            if (config.isBulkWrite() || !config.isUpdateAllDocuments()) {
                factory = (AbstractValuesProcessorFactory<T>) new SetOneModelProcessFactory();
            } else {
                factory = (AbstractValuesProcessorFactory<T>) new SetManyModelProcessFactory();
            }
            break;
        case UPSERT:
            factory = (AbstractValuesProcessorFactory<T>) new UpsertModelProcessFactory();
            break;
        case UPSERT_WITH_SET:
            if (config.isBulkWrite() || !config.isUpdateAllDocuments()) {
                factory = (AbstractValuesProcessorFactory<T>) new UpsertWithSetModelProcessFactory();
            } else {
                factory = (AbstractValuesProcessorFactory<T>) new UpsertWithSetManyModelProcessFactory();
            }
            break;
        default:
            throw new IllegalArgumentException();
        }
        return factory;
    }

    public static class InsertModelProcessorFactory implements AbstractValuesProcessorFactory<InsertOneModel<Document>> {

        @Override
        public ModelProducer<InsertOneModel<Document>> createProducer(final MongoDBOutputConfiguration configuration) {
            return new InsertModelProducer();
        }

        @Override
        public ModelWriter<InsertOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new InsertModelWriter(collection);
        }

    }

    public static class DeleteModelProcessorFactory implements AbstractValuesProcessorFactory<DeleteOneModel<Document>> {

        @Override
        public ModelProducer<DeleteOneModel<Document>> createProducer(final MongoDBOutputConfiguration configuration) {
            return new DeleteModelProducer(configuration);
        }

        @Override
        public ModelWriter<DeleteOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new DeleteModelWriter(collection);
        }

    }

    public static class UpdateModelProcessFactory implements AbstractValuesProcessorFactory<ReplaceOneModel<Document>> {

        @Override
        public ModelProducer<ReplaceOneModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new UpdateModelProducer(configuration);
        }

        @Override
        public ModelWriter<ReplaceOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new UpdateModelWriter(collection);
        }
    }

    public static class SetOneModelProcessFactory implements AbstractValuesProcessorFactory<UpdateOneModel<Document>> {

        @Override
        public ModelProducer<UpdateOneModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new SetOneModelProducer(configuration);
        }

        @Override
        public ModelWriter<UpdateOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new SetOneModelWriter(collection);
        }
    }

    public static class SetManyModelProcessFactory implements AbstractValuesProcessorFactory<UpdateManyModel<Document>> {

        @Override
        public ModelProducer<UpdateManyModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new SetManyModelProducer(configuration);
        }

        @Override
        public ModelWriter<UpdateManyModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new SetManyModelWriter(collection);
        }
    }

    public static class UpsertModelProcessFactory implements AbstractValuesProcessorFactory<ReplaceOneModel<Document>> {

        @Override
        public ModelProducer<ReplaceOneModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new UpsertModelProducer(configuration);
        }

        @Override
        public ModelWriter<ReplaceOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new UpsertModelWriter(collection);
        }
    }

    public static class UpsertWithSetModelProcessFactory implements AbstractValuesProcessorFactory<UpdateOneModel<Document>> {

        @Override
        public ModelProducer<UpdateOneModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new UpsertWithSetModelProducer(configuration);
        }

        @Override
        public ModelWriter<UpdateOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new UpsertWithSetModelWriter(collection);
        }
    }

    public static class UpsertWithSetManyModelProcessFactory implements AbstractValuesProcessorFactory<UpdateOneModel<Document>> {

        @Override
        public ModelProducer<UpdateOneModel<Document>> createProducer(MongoDBOutputConfiguration configuration) {
            return new UpsertWithSetModelProducer(configuration);
        }

        @Override
        public ModelWriter<UpdateOneModel<Document>> createWriter(MongoCollection<Document> collection) {
            return new UpsertWithSetManyModelWriter(collection);
        }
    }
}
