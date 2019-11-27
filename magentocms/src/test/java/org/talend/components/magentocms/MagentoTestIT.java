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
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.RestVersion;
import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.input.ConfigurationFilter;
import org.talend.components.magentocms.input.MagentoInputConfiguration;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.input.SelectionType;
import org.talend.components.magentocms.output.MagentoOutputConfiguration;
import org.talend.components.magentocms.service.http.BadRequestException;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.base.lang.exception.InvocationExceptionWrapper;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

@Slf4j
@DisplayName("Suite of test for the Magento components")
@WithComponents("org.talend.components.magentocms")
@ExtendWith(MagentoTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MagentoTestIT {

    @Injected
    private BaseComponentsHandler componentsHandler = null;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private MagentoHttpClientService magentoHttpClientService;

    private MagentoTestExtension.TestContext testContext;

    @BeforeAll
    private void init(MagentoTestExtension.TestContext testContext) {
        log.info("init: " + testContext.getMagentoAdminPassword());
        this.testContext = testContext;
    }

    @Test
    @DisplayName("Input. Get product by SKU")
    void inputComponentProductBySku() {
        log.info("Integration test 'Input. Get product by SKU' start ");
        MagentoInputConfiguration inputConfiguration = new MagentoInputConfiguration();
        inputConfiguration.getMagentoDataSet().setMagentoDataStore(testContext.getDataStoreSecure());
        inputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        inputConfiguration.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecure() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoInputConfiguration inputConfiguration = new MagentoInputConfiguration();

        inputConfiguration.getMagentoDataSet().setMagentoDataStore(testContext.getDataStore());
        inputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        inputConfiguration.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Get product by SKU. Non secure")
    void inputComponentProductBySkuNonSecureOauth1() {
        log.info("Integration test 'Input. Get product by SKU. Non secure' start ");
        MagentoInputConfiguration inputConfiguration = new MagentoInputConfiguration();
        inputConfiguration.getMagentoDataSet().setMagentoDataStore(testContext.getDataStoreOauth1());
        inputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        List<SelectionFilter> filterList = new ArrayList<>();
        SelectionFilter filter = new SelectionFilter("sku", "eq", "24-MB01");
        filterList.add(filter);
        inputConfiguration.setSelectionFilter(new ConfigurationFilter(SelectionFilterOperator.OR, filterList, null));

        final String config = SimpleFactory.configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                .connections().from("magento-input").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("Joust Duffle Bag", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Input. Bad credentials")
    void inputComponentBadCredentials() {
        log.info("Integration test 'Input. Bad credentials' start");
        AuthenticationLoginPasswordConfiguration authSettingsBad = new AuthenticationLoginPasswordConfiguration(
                testContext.getMagentoAdminName(), testContext.getMagentoAdminPassword() + "_make it bad");
        MagentoDataStore dataStoreBad = new MagentoDataStore(
                "http://" + testContext.getDockerHostAddress() + ":" + testContext.getMagentoHttpPort(), RestVersion.V1,
                AuthenticationType.LOGIN_PASSWORD, null, null, authSettingsBad);

        MagentoInputConfiguration inputConfiguration = new MagentoInputConfiguration();
        inputConfiguration.getMagentoDataSet().setMagentoDataStore(dataStoreBad);
        inputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        final String config = SimpleFactory.configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        try {
            Job.components().component("magento-input", "Magento://Input?" + config).component("collector", "test://collector")
                    .connections().from("magento-input").to("collector").build().run();
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof InvocationExceptionWrapper.ComponentException
                    && "org.talend.components.magentocms.service.http.BadCredentialsException"
                            .equals(((InvocationExceptionWrapper.ComponentException) e.getCause()).getOriginalType()));
        }
    }

    @ParameterizedTest
    @MethodSource("methodSourceDataStores")
    @DisplayName("Output. Write custom product")
    void outputComponent(MagentoDataStore dataStoreCustom) {
        log.info("Integration test 'Output. Write custom product' start. " + dataStoreCustom);
        MagentoOutputConfiguration outputConfiguration = new MagentoOutputConfiguration();
        outputConfiguration.getMagentoDataSet().setMagentoDataStore(dataStoreCustom);
        outputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        final String config = SimpleFactory.configurationByExample().forInstance(outputConfiguration).configured()
                .toQueryString();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("sku", "24-MB01_" + UUID.randomUUID().toString())
                .add("name", "Joust Duffle Bag_" + UUID.randomUUID().toString()).add("attribute_set_id", 15).add("price", 34)
                .add("status", 1).add("visibility", 4).add("type_id", "simple").add("created_at", "2018-08-01 13:28:05")
                .add("updated_at", "2018-08-01 13:28:05").build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter").component("magento-output", "Magento://Output?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("magento-output")
                .from("magento-output").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertTrue(res.iterator().next().containsKey("id"));
    }

    private Stream<Arguments> methodSourceDataStores() {
        return Stream.of(Arguments.of(testContext.getDataStore()), Arguments.of(testContext.getDataStoreSecure()),
                Arguments.of(testContext.getDataStoreOauth1()));
    }

    @Test
    @DisplayName("Input. Bad request")
    void inputBadRequestNoParameters() {
        log.info("Integration test 'Input. Bad request' start");
        MagentoInputConfiguration inputConfiguration = new MagentoInputConfiguration();
        inputConfiguration.getMagentoDataSet().setMagentoDataStore(testContext.getDataStore());
        inputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);

        ConfigurationHelper.setupServicesInput(inputConfiguration, magentoHttpClientService);

        Executable exec = () -> magentoHttpClientService.getRecords(inputConfiguration.getMagentoDataSet().getMagentoDataStore(),
                inputConfiguration.getMagentoDataSet().getMagentoUrl(), new TreeMap<>());
        Assertions.assertThrows(BadRequestException.class, exec);
    }

    @Test
    @DisplayName("Output. Bad request")
    void outputBadRequestNoParameters() {
        log.info("Integration test 'Output. Bad request' start");
        MagentoOutputConfiguration outputConfiguration = new MagentoOutputConfiguration();
        outputConfiguration.getMagentoDataSet().setMagentoDataStore(testContext.getDataStore());
        outputConfiguration.getMagentoDataSet().setSelectionType(SelectionType.PRODUCTS);
        ConfigurationHelper.setupServicesOutput(outputConfiguration, magentoHttpClientService);
        JsonObject dataList = jsonBuilderFactory.createObjectBuilder().add("bad_field", "").build();
        Executable exec = () -> magentoHttpClientService.postRecords(
                outputConfiguration.getMagentoDataSet().getMagentoDataStore(),
                outputConfiguration.getMagentoDataSet().getMagentoUrl(), dataList);
        Assertions.assertThrows(BadRequestException.class, exec);
    }
}
