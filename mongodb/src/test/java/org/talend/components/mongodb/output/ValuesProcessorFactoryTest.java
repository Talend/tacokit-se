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

import org.junit.jupiter.api.Test;
import org.talend.components.mongodb.dataset.MongoDBDataset;
import org.talend.components.mongodb.datastore.MongoDBDatastore;
import org.talend.components.mongodb.output.processor.AbstractValuesProcessorFactory;
import org.talend.components.mongodb.output.processor.ValuesProcessorsFactory;
import org.talend.components.mongodb.output.processor.impl.*;

import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;

public class ValuesProcessorFactoryTest {

    @Test
    public void testCreateInsertValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.INSERT);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.InsertModelProcessorFactory.class));

        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(InsertModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(InsertModelWriter.class));
    }

    @Test
    public void testCreateDeleteValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.DELETE);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.DeleteModelProcessorFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(DeleteModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(DeleteModelWriter.class));
    }

    @Test
    public void testCreateUpdateValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPDATE);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.UpdateModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(UpdateModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(UpdateModelWriter.class));
    }

    @Test
    public void testCreateSetValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.SET);
        config.setBulkWrite(true);
        config.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.SetOneModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(SetOneModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(SetOneModelWriter.class));

        config.setBulkWrite(false);
        config.setUpdateAllDocuments(false);
        valuesProcessorFactory = ValuesProcessorsFactory.createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.SetOneModelProcessFactory.class));

        config.setUpdateAllDocuments(true);
        valuesProcessorFactory = ValuesProcessorsFactory.createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.SetManyModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(SetManyModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(SetManyModelWriter.class));
    }

    @Test
    public void testCreateUpsertValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.UpsertModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(UpsertModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(UpsertModelWriter.class));
    }

    @Test
    public void testCreateUpsertWithSetValuesProcessorFactory() {
        MongoDBOutputConfiguration config = createConfiguration();
        config.setActionOnData(MongoDBOutputConfiguration.ActionOnData.UPSERT_WITH_SET);
        config.setBulkWrite(true);
        config.setBulkWriteType(MongoDBOutputConfiguration.BulkWriteType.ORDERED);
        AbstractValuesProcessorFactory<?> valuesProcessorFactory = ValuesProcessorsFactory
                .createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.UpsertWithSetModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(UpsertWithSetModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(UpsertWithSetModelWriter.class));

        config.setBulkWrite(false);
        config.setUpdateAllDocuments(false);
        valuesProcessorFactory = ValuesProcessorsFactory.createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.UpsertWithSetModelProcessFactory.class));

        config.setUpdateAllDocuments(true);
        valuesProcessorFactory = ValuesProcessorsFactory.createAbstractValuesProcessorFactory(config);
        assertThat(valuesProcessorFactory, instanceOf(ValuesProcessorsFactory.UpsertWithSetManyModelProcessFactory.class));
        assertThat(valuesProcessorFactory.createProducer(config), instanceOf(UpsertWithSetModelProducer.class));
        assertThat(valuesProcessorFactory.createWriter(null), instanceOf(UpsertWithSetManyModelWriter.class));
    }

    private MongoDBOutputConfiguration createConfiguration() {
        MongoDBDatastore datastore = new MongoDBDatastore();
        MongoDBDataset dataset = new MongoDBDataset();
        dataset.setDatastore(datastore);
        MongoDBOutputConfiguration config = new MongoDBOutputConfiguration();
        config.setDataset(dataset);
        config.setKeys(Arrays.asList("a", "b"));
        return config;
    }

}
