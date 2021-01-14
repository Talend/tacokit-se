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
import org.junit.Test;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

public class CosmosDBServiceTestIT extends CosmosDbTestBase {

    @Test
    public void cosmosDBSuccessfulConnectionTest() {
        Assert.assertEquals(HealthCheckStatus.Status.OK, service.healthCheck(dataStore).getStatus());
    }

    @Test
    public void cosmosDBFailConnectionTest() {
        dataStore.setPrimaryKey("fakeKey");
        HealthCheckStatus healthCheckStatus = service.healthCheck(dataStore);
        Assert.assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());
    }

    @Test
    public void cosmosDBnullIDConnectionTest() {
        dataStore.setDatabaseID("");
        HealthCheckStatus healthCheckStatus = service.healthCheck(dataStore);
        Assert.assertEquals(HealthCheckStatus.Status.KO, healthCheckStatus.getStatus());
    }

    @Test
    public void addColumnsTest() {
        dataSet.setUseQuery(true);
        dataSet.setQuery("SELECT {\"Name\":f.id, \"City\":f.address.city} AS Family\n" + "    FROM Families f");
        Schema schema = service.addColumns(dataSet);
        System.out.println(schema);
        Assert.assertNotNull(schema);
        Assert.assertEquals(Schema.Type.RECORD, schema.getType());
    }

}
