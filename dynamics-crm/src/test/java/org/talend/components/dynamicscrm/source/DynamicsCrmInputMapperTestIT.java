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
package org.talend.components.dynamicscrm.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.Arrays;
import java.util.List;

import javax.naming.AuthenticationException;
import javax.naming.ServiceUnavailableException;

import org.apache.olingo.client.api.domain.ClientEntity;
import org.apache.olingo.client.api.uri.FilterFactory;
import org.apache.olingo.client.core.uri.FilterFactoryImpl;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.dynamicscrm.DynamicsCrmTestBase;
import org.talend.components.dynamicscrm.dataset.DynamicsCrmDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithComponents("org.talend.components.dynamicscrm")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DynamicsCrmInputMapperTestIT extends DynamicsCrmTestBase {

    @BeforeAll
    public void prepareTestData() throws AuthenticationException, ServiceUnavailableException {
        init();
        ClientEntity entity = createTestEntity(client);

        client.insertEntity(entity);
    }

    @Test
    public void produce() {
        final DynamicsCrmDataset dataset = createDataset();
        final DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setDataset(dataset);
        configuration.setColumns(Arrays.asList("annualincome", "assistantname", "business2", "callback", "childrensnames",
                "company", "creditonhold"));
        FilterFactory filterFactory = new FilterFactoryImpl();
        configuration.setCustomFilter(true);
        configuration.setFilter(filterFactory.eq("company", company).build());

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("mycomponent", "Azure://AzureDynamics365Input?" + config) //
                .component("collector", "test://collector") //
                .connections() //
                .from("mycomponent") //
                .to("collector") //
                .build() //
                .run();
        final List<Record> records = components.getCollectedData(Record.class);
        assertNotNull(records);
        assertEquals(1, records.size());

        // Check the record value
        Record resultRecord = records.get(0);
        assertEquals(false, resultRecord.getBoolean("creditonhold"));
        assertEquals(2.0f, resultRecord.getFloat("annualincome"));
        assertEquals("assistant", resultRecord.getString("assistantname"));
        assertEquals("business2", resultRecord.getString("business2"));
        assertEquals("callback", resultRecord.getString("callback"));
        assertEquals("childrensnames", resultRecord.getString("childrensnames"));
        assertEquals(company, resultRecord.getString("company"));
    }

    @AfterAll
    public void clearData() throws ServiceUnavailableException {
        tearDown(client);
    }

}