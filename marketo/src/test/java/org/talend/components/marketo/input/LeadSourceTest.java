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
package org.talend.components.marketo.input;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.components.marketo.MarketoBaseTestIT;
import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeMode;
import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeRelative;
import org.talend.components.marketo.dataset.MarketoDataSet.LeadAction;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

import lombok.extern.slf4j.Slf4j;

@TestInstance(Lifecycle.PER_CLASS)
@Slf4j
@WithComponents("org.talend.components.marketo")
@HttpApi(useSsl = true)
class LeadSourceTest extends MarketoBaseTest {

    @Test
    void testSource() {
        this.dataSet.setListId("1011");
        this.dataSet.setDateTimeMode(DateTimeMode.relative);
        this.dataSet.setSinceDateTimeRelative(DateTimeRelative.PERIOD_AGO_2W);
        this.dataSet.setLeadAction(LeadAction.getLeadsByList);
        final LeadSource source = new LeadSource(this.inputConfiguration, this.service);

        source.init();
        Record record = source.next();
        Assertions.assertNotNull(record);
        Assertions.assertTrue(record.getBoolean("isLead"));
        while (record != null) {
            record = source.next();
        }
    }

    @Test
    void testSourceActivities() {
        this.dataSet.setListId("1011");

        this.dataSet.setDateTimeMode(DateTimeMode.absolute);
        this.dataSet.setSinceDateTimeAbsolute("2019-03-23");

        this.dataSet.setActivityTypeIds(Arrays.asList("1", "2", "3", "6", "7", "8", "9", "10", "22"));
        this.dataSet.setLeadAction(LeadAction.getLeadActivity);
        final LeadSource source = new LeadSource(this.inputConfiguration, this.service);

        source.init();
        source.next();
    }
}