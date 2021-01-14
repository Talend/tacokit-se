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
package org.talend.components.workday.input;

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayBaseTest;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.dataset.RAASLayout;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@Disabled
@HttpApi(useSsl = true)
@WithComponents("org.talend.components.workday")
class RAASProducerTest extends WorkdayBaseTest {

    private static WorkdayDataSet dataset;

    private static WorkdayConfiguration cfg;

    @Service
    private WorkdayReaderService service;

    @BeforeEach
    private void init() {
        RAASProducerTest.cfg = new WorkdayConfiguration();
        RAASProducerTest.dataset = new WorkdayDataSet();
        RAASProducerTest.dataset.setDatastore(this.buildDataStore());
        RAASProducerTest.dataset.setMode(WorkdayDataSet.WorkdayMode.RAAS);
        RAASProducerTest.dataset.setRaas(new RAASLayout());
        cfg.setDataSet(dataset);
    }

    @Test
    void nextOK() {

        RAASProducerTest.dataset.getRaas().setUser("lmcneil");
        RAASProducerTest.dataset.getRaas().setReport("billingReport");

        WorkdayProducer producer = new WorkdayProducer(cfg, service);
        JsonObject o = producer.next();
        Assertions.assertNotNull(o);

        JsonObject o2 = producer.next();
        Assertions.assertNotNull(o2);
    }

    @Test
    void withSpace() {
        RAASProducerTest.dataset.getRaas().setUser("lmcneil");
        RAASProducerTest.dataset.getRaas().setReport("billing report");

        WorkdayProducer producer = new WorkdayProducer(cfg, service);
        JsonObject o = producer.next();
        Assertions.assertNotNull(o);

        JsonObject o2 = producer.next();
        Assertions.assertNotNull(o2);
    }

    @Test
    void nextError() {
        RAASProducerTest.dataset.getRaas().setUser("omcneil");
        RAASProducerTest.dataset.getRaas().setReport("falseReport");

        WorkdayProducer producer = new WorkdayProducer(cfg, service);

        Assertions.assertThrows(WorkdayException.class, producer::next);
    }
}