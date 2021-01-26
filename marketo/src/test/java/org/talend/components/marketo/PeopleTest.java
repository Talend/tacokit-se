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
package org.talend.components.marketo;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration.OutputAction;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@WithComponents("org.talend.components.marketo")
@HttpApi(useSsl = true)
public class PeopleTest extends MarketoBaseTest {

    public static final String LIST_ID_COMPQA = "1011";

    Record r1, r2, r3;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        r1 = recordBuilderFactory.newRecordBuilder() //
                .withString("email", "release-1.2.0-test-r1@talend.com") //
                .withString("firstName", "Talend") //
                .withString("lastName", "IT-1") //
                .build();
        r2 = recordBuilderFactory.newRecordBuilder() //
                .withString("email", "release-1.2.0-test-r2@talend.com") //
                .withString("firstName", "Talend") //
                .withString("lastName", "IT-2") //
                .build();

        r3 = recordBuilderFactory.newRecordBuilder() //
                .withString("email", "Release_10@talend.com") //
                .withString("firstName", "firstName") //
                .withString("lastName", "lastName") //
                .build();
    }

    @Test
    void createOnlyPeopleWithListId() {
        log.warn("[createOnlyPeopleWithListId] using endpoint: {}", dataSet.getDataStore().getEndpoint());
        dataSet.setListId(LIST_ID_COMPQA);
        outputConfiguration.setDataSet(dataSet);
        outputConfiguration.setAction(OutputAction.createOnly);
        outputConfiguration.setLookupField(MarketoApiConstants.ATTR_EMAIL);
        List<Record> records = new ArrayList<>(1);
        records.add(r3);
        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        handler.setInputData(records);
        Job.components() //
                .component("in", "test://emitter") //
                .component("out", "Marketo://Output?" + config) //
                .connections() //
                .from("in") //
                .to("out") //
                .build().run();
        handler.resetState();
        final List<Record> results = handler.getCollectedData(Record.class);
        assertNotNull(results);
    }
}
