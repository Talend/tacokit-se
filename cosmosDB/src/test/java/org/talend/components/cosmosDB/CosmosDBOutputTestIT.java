/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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

import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClientException;
import org.junit.jupiter.api.Assertions;
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
    public void outputTest() throws DocumentClientException {
        config.setAutoIDGeneration(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData(1).get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        Document document = cosmosTestUtils.readDocuments(collectionID, record.getString("id"), "firstfirst");
        Assertions.assertTrue(this.recordEqual(record, document));

    }

    @Test
    public void createCollectionTest() throws DocumentClientException {
        config.setAutoIDGeneration(true);
        dataSet.setCollectionID("pyzhouTest2");
        config.setDataset(dataSet);
        config.setPartitionKey("/id2");
        config.setCreateCollection(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData(1).get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        boolean exist = cosmosTestUtils.isCollectionExist("pyzhouTest2");
        Assertions.assertTrue(exist);
        if (exist)
            cosmosTestUtils.deleteCollection("pyzhouTest2");
    }

    @Test
    public void DeleteTest() {
        config.setAutoIDGeneration(true);
        config.setDataAction(DataAction.DELETE);
        config.setPartitionKeyForDelete("lastName");
        config.setCreateCollection(true);
        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData3().get(0);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();
        try {
            cosmosTestUtils.readDocuments(collectionID, "Andersen.1", "Andersen");
            Assertions.fail();
        } catch (DocumentClientException e) {
            Assertions.assertEquals(404, e.getStatusCode());
        }

    }
}
