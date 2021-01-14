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
package org.talend.components.salesforce;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.talend.components.salesforce.configuration.InputModuleConfig;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.Data;

@Data
@WithComponents("org.talend.components.salesforce")
public class SalesforceTestBase implements Serializable {

    public static String USER_ID;

    public static String PASSWORD;

    public static String SECURITY_KEY;

    public static String EXPIRED_USER_ID;

    public static String EXPIRED_PASSWORD;

    public static String EXPIRED_SECURITY_KEY;

    static {
        final MavenDecrypter decrypter = new MavenDecrypter();
        final Server serverWithPassword = decrypter.find("salesforce-password");
        final Server serverWithSecuritykey = decrypter.find("salesforce-securitykey");
        USER_ID = serverWithPassword.getUsername();
        PASSWORD = serverWithPassword.getPassword();
        SECURITY_KEY = serverWithSecuritykey.getPassword();
        final Server expiredServerWithPassword = decrypter.find("salesforce-password-expired");
        final Server expiredServerWithSecuritykey = decrypter.find("salesforce-securitykey-expired");
        EXPIRED_USER_ID = expiredServerWithPassword.getUsername();
        EXPIRED_PASSWORD = expiredServerWithPassword.getPassword();
        EXPIRED_SECURITY_KEY = expiredServerWithSecuritykey.getPassword();
    }

    @Injected
    private BaseComponentsHandler componentsHandler;

    protected void cleanTestRecords(String module, String condition) {

        final ModuleDataSet inputDataSet = new ModuleDataSet();
        inputDataSet.setModuleName(module);
        inputDataSet.setSelectColumnNames(Arrays.asList("Id"));
        inputDataSet.setDataStore(getDataStore());
        inputDataSet.setCondition(condition);

        InputModuleConfig inputModuleConfig = new InputModuleConfig();
        inputModuleConfig.setDataSet(inputDataSet);
        final String inputConfig = configurationByExample().forInstance(inputModuleConfig).configured().toQueryString();

        // output
        final OutputConfig configuration = new OutputConfig();
        final ModuleDataSet outDataSet = new ModuleDataSet();
        outDataSet.setModuleName(module);
        outDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfig.OutputAction.DELETE);
        configuration.setModuleDataSet(outDataSet);

        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + inputConfig)
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + outputConfig).connections()
                .from("salesforce-input").to("salesforce-output").build().run();
        getComponentsHandler().resetState();
    }

    protected void checkModuleData(String module, String condition, int expected) {
        final ModuleDataSet dataSet = new ModuleDataSet();
        dataSet.setModuleName(module);
        dataSet.setSelectColumnNames(singletonList("Id"));
        dataSet.setDataStore(getDataStore());
        dataSet.setCondition(condition);

        InputModuleConfig inputModuleConfig = new InputModuleConfig();
        inputModuleConfig.setDataSet(dataSet);
        final String config = configurationByExample().forInstance(inputModuleConfig).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + config)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertEquals(expected, res.size());
        getComponentsHandler().resetState();
    }

    public BasicDataStore getDataStore() {
        BasicDataStore dataStore = new BasicDataStore();
        dataStore.setEndpoint(URL);
        dataStore.setUserId(USER_ID);
        dataStore.setPassword(PASSWORD);
        dataStore.setSecurityKey(SECURITY_KEY);
        return dataStore;
    }

}