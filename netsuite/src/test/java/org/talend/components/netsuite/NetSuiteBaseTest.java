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
package org.talend.components.netsuite;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.junit.ClassRule;
import org.junit.jupiter.api.BeforeAll;
import org.talend.components.netsuite.dataset.NetSuiteDataSet;
import org.talend.components.netsuite.dataset.NetSuiteInputProperties;
import org.talend.components.netsuite.dataset.NetSuiteOutputProperties;
import org.talend.components.netsuite.dataset.SearchConditionConfiguration;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.ApiVersion;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.LoginType;
import org.talend.components.netsuite.service.Messages;
import org.talend.components.netsuite.service.NetSuiteClientConnectionService;
import org.talend.components.netsuite.service.NetSuiteService;
import org.talend.components.netsuite.test.TestCollector;
import org.talend.components.netsuite.test.TestEmitter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class NetSuiteBaseTest {

    private static final String FAMILY_NAME = "NetSuite";

    private static final String EMITTER = "Input";

    private static final String COLLECTOR = "Output";

    public static final String TEST_FAMILY_NAME = "NetSuiteTest";

    public static final String TEST_EMITTER = "TestEmitter";

    public static final String TEST_COLLECTOR = "TestCollector";

    protected static String NETSUITE_ACCOUNT;

    protected static String NETSUITE_ROLE_ID;

    protected static NetSuiteDataStore dataStore;

    protected static NetSuiteService netSuiteService;

    protected static NetSuiteClientConnectionService netSuiteClientConnectionService;

    protected static Messages i18n;

    protected static RecordBuilderFactory factory;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.netsuite");

    @BeforeAll
    public static void setupOnce() throws IOException {
        readPropertiesFile();

        i18n = COMPONENT.findService(Messages.class);
        factory = COMPONENT.findService(RecordBuilderFactory.class);
        netSuiteService = COMPONENT.findService(NetSuiteService.class);
        netSuiteClientConnectionService = COMPONENT.findService(NetSuiteClientConnectionService.class);

        final MavenDecrypter decrypter = new MavenDecrypter();
        Server consumer = decrypter.find("netsuite.consumer");
        Server token = decrypter.find("netsuite.token");
        dataStore = new NetSuiteDataStore();
        dataStore.setApiVersion(ApiVersion.V2019_2);
        dataStore.setAccount(NETSUITE_ACCOUNT);
        dataStore.setLoginType(LoginType.TBA);
        dataStore.setConsumerKey(consumer.getUsername());
        dataStore.setConsumerSecret(consumer.getPassword());
        dataStore.setTokenId(token.getUsername());
        dataStore.setTokenSecret(token.getPassword());
    }

    private static void readPropertiesFile() throws IOException {
        try (InputStream is = ClassLoader.getSystemResourceAsStream("connection.properties")) {
            Properties props = new Properties();
            props.load(is);
            NETSUITE_ACCOUNT = props.getProperty("netsuite.account", "");
            NETSUITE_ROLE_ID = props.getProperty("netsuite.roleId", "");
        }
    }

    private String getComponentName(String familyName, String componentType) {
        return String.format("%s://%s", familyName, componentType);
    }

    private String getComponentWithConfig(String component, String config) {
        return String.format("%s?%s", component, config);
    }

    private void buildAndRunPipeline(String emitter, String collector) {
        Job.components().component("emitter", emitter).component("collector", collector).connections().from("emitter")
                .to("collector").build().run();
    }

    protected void buildAndRunCollectorJob(NetSuiteOutputProperties properties, List<Record> records) {
        records.forEach(TestEmitter::addRecord);
        buildAndRunPipeline(getComponentName(TEST_FAMILY_NAME, TEST_EMITTER),
                getComponentWithConfig(getComponentName(FAMILY_NAME, COLLECTOR), getQueryProperties(properties)));
        TestEmitter.reset();
    }

    protected List<Record> buildAndRunEmitterJob(NetSuiteInputProperties properties) {
        TestCollector.reset();
        buildAndRunPipeline(getComponentWithConfig(getComponentName(FAMILY_NAME, EMITTER), getQueryProperties(properties)),
                getComponentName(TEST_FAMILY_NAME, TEST_COLLECTOR));
        return new ArrayList<>(TestCollector.getData());
    }

    protected List<SearchConditionConfiguration> createSearchConditionConfiguration(String value) {
        return Collections.singletonList(new SearchConditionConfiguration("internalId", "List.anyOf", value, ""));
    }

    private <T> String getQueryProperties(T properties) {
        return configurationByExample().forInstance(properties).configured().toQueryString() + "&configuration.$maxBatchSize=200";
    }

    protected NetSuiteOutputProperties createOutputProperties() {
        NetSuiteOutputProperties outputProperties = new NetSuiteOutputProperties();
        NetSuiteDataSet dataSet = createDefaultDataSet();
        outputProperties.setDataSet(dataSet);
        return outputProperties;
    }

    protected NetSuiteInputProperties createInputProperties() {
        NetSuiteInputProperties inputProperties = new NetSuiteInputProperties();
        NetSuiteDataSet dataSet = createDefaultDataSet();
        inputProperties.setDataSet(dataSet);
        return inputProperties;
    }

    protected NetSuiteDataSet createDefaultDataSet() {
        NetSuiteDataSet dataSet = new NetSuiteDataSet();
        dataSet.setDataStore(dataStore);
        return dataSet;
    }
}
