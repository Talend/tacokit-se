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
package org.talend.components.ftp.source;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.slf4j.impl.StaticLoggerBinder;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator;
import org.talend.components.ftp.dataset.FTPDataSet;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.jupiter.FtpFile;
import org.talend.components.ftp.jupiter.FtpServer;
import org.talend.components.ftp.service.FTPService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.File;
import java.util.List;

@Slf4j

@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})
@WithComponents(value = "org.talend.components.ftp")
@FtpFile(base = "fakeFTP/", port = 4523)
public class FTPInputTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    private FTPInputConfiguration configuration;

    @BeforeEach
    void buildConfig() {

        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();

        componentsHandler.injectServices(this);

        FTPDataStore datastore = new FTPDataStore();
        datastore.setHost("localhost");
        datastore.setUseCredentials(true);
        datastore.setUsername(FtpServer.USER);
        datastore.setPassword(FtpServer.PASSWD);
        datastore.setPort(4523);

        FTPDataSet dataset = new FTPDataSet();
        dataset.setDatastore(datastore);
        dataset.setPath("/communes");

        configuration = new FTPInputConfiguration();
        configuration.setDataSet(dataset);
        configuration.setDebug(true);
    }

    @EnvironmentalTest
    public void csvTest() {
        configuration.getDataSet().setPath("/communes");
        configuration.getDataSet().setFormat(FTPDataSet.Format.CSV);
        FieldSeparator fieldSeparator = new FieldSeparator();
        fieldSeparator.setFieldSeparatorType(FieldSeparator.Type.SEMICOLON);
        configuration.getDataSet().getCsvConfiguration().setFieldSeparator(fieldSeparator);
        configuration.getDataSet().getCsvConfiguration().setLineConfiguration(new LineConfiguration());

        String configURI = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        try {
            Job.components().component("input", "FTP://FTPInput?" + configURI).component("output", "test://collector")
                    .connections().from("input").to("output").build().run();
        } catch (Exception e) {
            Assertions.fail(e);
        }

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertNotNull(records);
        Assertions.assertEquals(207, records.size());
    }

    @EnvironmentalTest
    public void csvOneFileTest() {
        configuration.getDataSet().setPath("/communes/communes_0.csv");
        configuration.getDataSet().setFormat(FTPDataSet.Format.CSV);
        FieldSeparator fieldSeparator = new FieldSeparator();
        fieldSeparator.setFieldSeparatorType(FieldSeparator.Type.SEMICOLON);
        configuration.getDataSet().getCsvConfiguration().setFieldSeparator(fieldSeparator);
        configuration.getDataSet().getCsvConfiguration().setLineConfiguration(new LineConfiguration());

        String configURI = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        try {
            Job.components().component("input", "FTP://FTPInput?" + configURI).component("output", "test://collector")
                    .connections().from("input").to("output").build().run();
        } catch (Exception e) {
            Assertions.fail(e);
        }

        List<Record> records = componentsHandler.getCollectedData(Record.class);

        Assertions.assertNotNull(records);
        Assertions.assertEquals(49, records.size());
    }
}
