/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.cosmosDB;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.cosmosDB.output.CosmosDBOutput;
import org.talend.components.cosmosDB.output.CosmosDBOutputConfiguration;
import org.talend.components.cosmosDB.output.DataAction;
import org.talend.sdk.component.api.record.Record;

public class CosmosDBOutputTestIT extends CosmosDbTestBase {

    CosmosDBOutputConfiguration config;

    @Before
    public void prepare() {
        super.prepare();
        config = new CosmosDBOutputConfiguration();
        config.setDataset(dataSet);

    }

    @Test
    public void outputTest() {
        config.setAutoIDGeneration(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData(1).get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        // no Exception means success
    }

    @Test
    public void createCollectionTest() {
        config.setAutoIDGeneration(true);
        dataStore.setDatabaseID("ComponentQA");
        dataSet.setDatastore(dataStore);
        dataSet.setCollectionID("pyzhouTest2");
        config.setDataset(dataSet);
        config.setPartitionKey("/id2");
        config.setCreateCollection(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData(1).get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        // no Exception means success
    }

    @Test
    public void DeleteTest() {
        config.setAutoIDGeneration(true);
        dataStore.setDatabaseID("ComponentQA");
        dataSet.setDatastore(dataStore);
        dataSet.setCollectionID("Test1");
        config.setDataAction(DataAction.DELETE);
        config.setDataset(dataSet);
        config.setPartitionKeyForDelete("address");
        config.setCreateCollection(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData3().get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        // no Exception means success
    }
}
