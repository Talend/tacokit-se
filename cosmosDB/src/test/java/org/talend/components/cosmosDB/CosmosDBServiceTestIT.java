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

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

public class CosmosDBServiceTestIT extends CosmosDbTestBase {

    @Test
    public void cosmosDBSuccessfulConnectionTest() {
        Assert.assertEquals(HealthCheckStatus.Status.OK, service.healthCheck(dataStore).getStatus());
    }

    @Test
    public void cosmosDBFailConnectionTest() {
        dataStore.setPrimaryKey("fakeKey");
        Assert.assertEquals(HealthCheckStatus.Status.KO, service.healthCheck(dataStore).getStatus());
    }

    @Test
    public void addColumnsTest() {

        Assert.assertNotNull(service.addColumns(dataSet));

    }

    @Test
    public void record2JSONObjectTest() {
        Record record = createData(1).get(0);
        JSONObject actual = service.record2JSONObject(record);
        Assert.assertEquals(1, actual.getInt("id"));
        Assert.assertEquals(record.getDateTime("Date1").toString(), actual.get("Date1"));
    }

}
