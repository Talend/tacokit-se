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
package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.magentocms.common.MagentoDataSet;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
@ExtendWith(MagentoTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MagentoServiceTestIT {

    @Service
    private MagentoCmsService magentoCmsService;

    private MagentoTestExtension.TestContext testContext;

    @BeforeAll
    private void init(MagentoTestExtension.TestContext testContext) {
        log.info("init: " + testContext.getMagentoAdminPassword());
        this.testContext = testContext;
    }

    @Test
    @DisplayName("Schema discovery")
    void schemaDiscoveryTest() {
        log.info("Integration test 'Schema discovery' start ");
        MagentoDataSet dataSet = new MagentoDataSet();
        dataSet.setMagentoDataStore(testContext.getDataStoreSecure());
        dataSet.setSelectionType(SelectionType.PRODUCTS);

        Schema schema = magentoCmsService.guessTableSchema(dataSet);
        Assertions.assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "sku", "name")));
    }

    @Test
    @DisplayName("Health check")
    void healthCheckTest() {
        log.info("Integration test 'Health Check' start ");
        HealthCheckStatus healthCheckStatus = magentoCmsService.validateBasicConnection(testContext.getDataStoreSecure());
        Assertions.assertEquals(HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }
}
