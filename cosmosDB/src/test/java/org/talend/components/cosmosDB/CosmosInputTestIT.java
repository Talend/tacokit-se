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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.talend.components.cosmosDB.input.CosmosDBInput;
import org.talend.components.cosmosDB.input.CosmosDBInputConfiguration;
import org.talend.sdk.component.api.record.Record;

public class CosmosInputTestIT extends CosmosDbTestBase {

    CosmosDBInputConfiguration config;

    @Before
    public void prepare() {
        super.prepare();
        config = new CosmosDBInputConfiguration();
    }

    @Test
    public void QueryTest() {
        dataSet.setUseQuery(true);
        dataSet.setQuery("SELECT {\"Name\":f.id, \"City\":f.address.city} AS Family\n" + " FROM " + collectionID + " f\n"
                + " WHERE f.address.city = f.address.state");
        config.setDataset(dataSet);
        CosmosDBInput input = new CosmosDBInput(config, service, recordBuilderFactory, i18n);
        input.init();
        Record next = input.next();
        System.out.println(next);
        input.release();
        Assert.assertEquals("Wakefield.7", next.getRecord("Family").getString("Name"));

    }

    @Test
    public void withoutQueryTest() {
        dataSet.setUseQuery(false);
        dataSet.setQuery("SELECT {\"Name\":f.id, \"City\":f.address.city} AS Family   FROM " + collectionID
                + " f  WHERE f.address.county = \"Manhattan\"");
        config.setDataset(dataSet);
        CosmosDBInput input = new CosmosDBInput(config, service, recordBuilderFactory, i18n);
        input.init();
        Record next = input.next();
        input.release();
        Assert.assertNotNull(next);
    }
}
