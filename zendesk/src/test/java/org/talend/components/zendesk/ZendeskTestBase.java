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
package org.talend.components.zendesk;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import javax.json.JsonReaderFactory;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.talend.components.zendesk.common.AuthenticationApiTokenConfiguration;
import org.talend.components.zendesk.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.zendesk.common.ZendeskDataSet;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.components.zendesk.messages.Messages;
import org.talend.components.zendesk.service.ZendeskService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ServiceInjectionRule;
import org.talend.sdk.component.junit.SimpleComponentRule;

public class ZendeskTestBase {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.zendesk");

    @Rule
    public final ServiceInjectionRule injections = new ServiceInjectionRule(COMPONENT_FACTORY, this);

    @Service
    protected RecordBuilderFactory recordBuilderFactory;

    @Service
    protected ZendeskService zendeskService;

    @Service
    protected Messages i18n;

    protected static final String uuid = UUID.randomUUID().toString().replaceAll("-", "");

    protected ZendeskDataStore datastore = new ZendeskDataStore();

    protected ZendeskDataSet dataset = new ZendeskDataSet();

    String url;

    String userName;

    String password;

    String tocken;

    protected ClientAndServer mockServer;

    @Before
    public void startMockServer() {
        mockServer = startClientAndServer(1080);
    }

    @After
    public void stopMockServer() {
        mockServer.stop();
    }

    private void loadProperties() {
        // Properties prop = new Properties();
        // java.io.InputStream input = null;
        // try {
        // input = new FileInputStream(System.getenv("ENV") + "/tacokit_properties.txt");
        // prop.load(input);
        // for (String name : prop.stringPropertyNames()) {
        // System.setProperty(name, prop.getProperty(name));
        // }
        // } catch (java.io.IOException ex) {
        // System.err.println("Did not find azure properties, you can still pass them with -D");
        // }
        url = System.getProperty("zendesk.url", "http://localhost:1080/");
        userName = System.getProperty("zendesk.userName", "pyzhou@talend.com");
        password = System.getProperty("zendesk.password", "Talend");
        tocken = System.getProperty("zendesk.APItocken", "7F6yKDVhqwSNkJ5ldpB4FjSyoxAwli8i0nGgGc97");
        // System.setProperty("talend.junit.http.capture", "true");
    }

    @Before
    public void prepare() {
        loadProperties();
        datastore.setServerUrl(url);
        AuthenticationLoginPasswordConfiguration alp = new AuthenticationLoginPasswordConfiguration();
        alp.setAuthenticationLogin(userName);
        alp.setAuthenticationPassword(password);
        datastore.setAuthenticationLoginPasswordConfiguration(alp);
        AuthenticationApiTokenConfiguration aac = new AuthenticationApiTokenConfiguration();
        aac.setAuthenticationLogin(userName);
        aac.setApiToken(tocken);
        datastore.setAuthenticationApiTokenConfiguration(aac);
        dataset.setDataStore(datastore);

    }

    protected List<Record> createData(int i) {
        List records = new ArrayList(i);
        for (; i > 0; i--) {
            Record record = recordBuilderFactory.newRecordBuilder().withLong("requester_id", 902315752386L).withLong("id", i)
                    .withString("subject", "This is a Test " + i).withString("description", "dudulu").build();
            records.add(record);
        }
        return records;
    }

    protected String getResourceString(String path) throws IOException {
        URL resource = this.getClass().getResource(path);
        return FileUtils.readFileToString(new File(resource.getFile()), "UTF-8");
    }

}
