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

import com.microsoft.azure.documentdb.Document;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.talend.components.cosmosDB.output.CosmosDBOutput;
import org.talend.components.cosmosDB.output.CosmosDBOutputConfiguration;
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

        CosmosDBOutput cosmosDBOutput = new CosmosDBOutput(config, service, i18n);
        cosmosDBOutput.init();
        Record record = createData(1).get(0);
        System.out.println(record);
        cosmosDBOutput.onNext(record);
        cosmosDBOutput.release();

    }

    @Test
    public void Documenttest() {

        Record record = createData(1).get(0);
        System.out.println(record);
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("id2", "dsdsd");
        System.out.println(jsonObject);
        Document document = new Document(record.toString());
        System.out.println(document);

    }
}
