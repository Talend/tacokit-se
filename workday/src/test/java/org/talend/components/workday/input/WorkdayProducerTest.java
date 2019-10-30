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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.WorkdayException;
import org.talend.components.workday.dataset.WQLLayout;
import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.service.ConfigHelper;
import org.talend.components.workday.service.WorkdayReaderService;

import javax.json.JsonObject;

class WorkdayProducerTest {

    private static WorkdayDataSet dataset;

    private static WorkdayConfiguration cfg;

    private static WorkdayReaderService service;

    @BeforeAll
    private static void init() throws NoSuchFieldException, IllegalAccessException {
        WorkdayProducerTest.service = ConfigHelper.buildReader();

        WorkdayProducerTest.cfg = new WorkdayConfiguration();
        WorkdayProducerTest.dataset = new WorkdayDataSet();
        WorkdayProducerTest.dataset.setDatastore(ConfigHelper.buildDataStore());
        WorkdayProducerTest.dataset.setMode(WorkdayDataSet.WorkdayMode.WQL);
        WorkdayProducerTest.dataset.setWql(new WQLLayout());
        WorkdayProducerTest.cfg.setDataSet(dataset);
    }

    @Test
    void nextOK() {
        String query = "SELECT accountCurrency, bankAccountSecuritySegment, priorDayAccountBalance " + "FROM financialAccounts";
        WorkdayProducerTest.dataset.getWql().setQuery(query);

        WorkdayProducer producer = new WorkdayProducer(cfg, service);
        JsonObject o = producer.next();
        Assertions.assertNotNull(o);
    }

    @Test
    void nextError() {
        String query = "SELECT accountCurrency, bankAccountSecuritySegment, priorDayAccountBalance "
                + "FROM UnkownfinancialAccounts";
        WorkdayProducerTest.dataset.getWql().setQuery(query);
        WorkdayProducer producer = new WorkdayProducer(cfg, service);

        Assertions.assertThrows(WorkdayException.class, producer::next);
    }
}