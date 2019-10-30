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
package org.talend.components.workday.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.dataset.WorkdayServiceDataSet;
import org.talend.components.workday.datastore.Token;
import org.talend.components.workday.datastore.WorkdayDataStore;
import org.talend.components.workday.service.WorkdayReaderService;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.time.Instant;

class WorkdayServiceProducerSerializerTest {

    @Test
    public void serialProducer() throws Exception {
        InputConfiguration cfg = new InputConfiguration();
        WorkdayReaderService service = new WorkdayReaderService();

        WorkdayServiceDataSet ds = new WorkdayServiceDataSet();
        cfg.setDataSet(ds);
        ds.setService("hello/v1/xx");
        WorkdayDataStore store = new WorkdayDataStore();
        ds.setDatastore(store);
        store.setTenantAlias("myalias");
        store.setEndpoint("http://myendpoint");
        store.setClientId("clientid");
        store.setClientSecret("secret");
        store.setAuthEndpoint("http://myauthendpoint");
        store.setToken(new Token("123", "auth", Instant.now()));

        WorkdayServiceProducer producer = new WorkdayServiceProducer(cfg, service);
        producer.init();

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bytes);
        output.writeObject(producer);

        ByteArrayInputStream in = new ByteArrayInputStream(bytes.toByteArray());
        ObjectInputStream input = new ObjectInputStream(in);
        Object o = input.readObject();

        Assertions.assertNotNull(o);
        Assertions.assertEquals(WorkdayServiceProducer.class, o.getClass());

        WorkdayServiceProducer producer1 = (WorkdayServiceProducer) o;

        Field cfgField = WorkdayServiceProducer.class.getDeclaredField("inputConfig");
        cfgField.setAccessible(true);
        Object objCfg = cfgField.get(producer1);
        Assertions.assertNotNull(objCfg);

        InputConfiguration cfg2 = (InputConfiguration) objCfg;
        Assertions.assertEquals(cfg, cfg2);
    }

}