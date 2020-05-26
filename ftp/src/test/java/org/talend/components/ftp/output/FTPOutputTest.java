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
package org.talend.components.ftp.output;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.slf4j.impl.StaticLoggerBinder;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.ftp.dataset.FTPDataSet;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.jupiter.FtpFile;
import org.talend.components.ftp.jupiter.FtpServer;
import org.talend.components.ftp.source.FTPInputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.SimpleFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})
@WithComponents(value = "org.talend.components.ftp")
@FtpFile(base = "fakeFTP/", port = 4522)
public class FTPOutputTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    public RecordBuilderFactory rbf;

    private FTPOutputConfiguration configuration;

    @Rule
    public final SimpleComponentRule COMPONENTS = new SimpleComponentRule("org.talend.sdk.component.mycomponent");

    @BeforeEach
    void buildConfig() {

        final StaticLoggerBinder binder = StaticLoggerBinder.getSingleton();

        componentsHandler.injectServices(this);

        FTPDataStore datastore = new FTPDataStore();
        datastore.setHost("localhost");
        datastore.setPort(4522);
        datastore.setUseCredentials(true);
        datastore.setUsername(FtpServer.USER);
        datastore.setPassword(FtpServer.PASSWD);

        FTPDataSet dataset = new FTPDataSet();
        dataset.setDatastore(datastore);

        configuration = new FTPOutputConfiguration();
        configuration.setDataSet(dataset);
        configuration.setDebug(true);
    }

    @EnvironmentalTest
    public void testRecordLimit(UnixFakeFileSystem fileSystem) {
        String path = "/out1";
        fileSystem.add(new DirectoryEntry(path));
        int nbRecords = 210;
        int expectedFiles = 5;

        configuration.getDataSet().setPath(path);
        configuration.getDataSet().setFormat(FTPDataSet.Format.CSV);
        configuration.getDataSet().getCsvConfiguration().setLineConfiguration(new LineConfiguration());
        configuration.setLimitBy(FTPOutputConfiguration.LimitBy.RECORDS);
        configuration.setRecordsLimit(50);

        String configURI = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("k").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("v").withType(Schema.Type.STRING).build()).build();

        List<Record> inputData = IntStream.range(0, nbRecords)
                .mapToObj(i -> rbf.newRecordBuilder(schema).withString("k", "entry" + i).withString("v", "value" + i).build())
                .collect(Collectors.toList());

        COMPONENTS.setInputData(inputData);

        Job.components().component("source", "test://emitter").component("output", "FTP://FTPOutput?" + configURI).connections()
                .from("source").to("output").build().run();

        // Waiting for completion
        List<FileEntry> files = fileSystem.listFiles(path);
        int nbFiles = files.size();
        int nbRetry = 0;
        int maxNbRetries = 5;
        while (nbFiles < expectedFiles && nbRetry < maxNbRetries) {
            files = fileSystem.listFiles(path);
            nbFiles = files.size();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (nbRetry > maxNbRetries) {
            Assertions.fail("Wrong number of files generated : " + nbFiles + " instead of " + expectedFiles);
        }

        List<String> csvLines = new ArrayList<>();
        files.stream().map(FileEntry::createInputStream).map(is -> {
            try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int read = 0;
                while ((read = is.read(buffer)) > 0) {
                    bout.write(buffer, 0, read);
                    bout.flush();
                }
                return bout.toByteArray();
            } catch (IOException ioe) {
                log.error(ioe.getMessage(), ioe);
                return new byte[0];
            }
        }).map(b -> new String(b, StandardCharsets.ISO_8859_1)).forEach(s -> {
            Arrays.stream(s.split("\r\n")).filter(l -> !"".equals(l.trim())).forEach(csvLines::add);
        });

        Assertions.assertEquals(nbRecords, csvLines.size(), "Wrong number of lines");
    }

    @EnvironmentalTest
    public void testSizeLimit(UnixFakeFileSystem fileSystem) {
        String path = "/out2";
        fileSystem.add(new DirectoryEntry(path));
        int nbRecords = 200;
        int expectedFiles = 4;

        configuration.getDataSet().setPath(path);
        configuration.getDataSet().setFormat(FTPDataSet.Format.CSV);
        configuration.getDataSet().getCsvConfiguration().setLineConfiguration(new LineConfiguration());
        configuration.setLimitBy(FTPOutputConfiguration.LimitBy.SIZE);
        configuration.setSizeLimit(1);
        configuration.setSizeUnit(FTPOutputConfiguration.SizeUnit.KB);

        String configURI = SimpleFactory.configurationByExample().forInstance(configuration).configured().toQueryString();

        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("k").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("v").withType(Schema.Type.STRING).build()).build();

        List<Record> inputData = IntStream.range(0, nbRecords)
                .mapToObj(i -> rbf.newRecordBuilder(schema).withString("k", "entry" + i).withString("v", "value" + i).build())
                .collect(Collectors.toList());

        COMPONENTS.setInputData(inputData);

        Job.components().component("source", "test://emitter").component("output", "FTP://FTPOutput?" + configURI).connections()
                .from("source").to("output").build().run();

        // Waiting for completion
        List<FileEntry> files = fileSystem.listFiles(path);
        int nbFiles = files.size();
        int nbRetry = 0;
        int maxNbRetries = 5;
        while (nbFiles < expectedFiles && nbRetry < maxNbRetries) {
            files = fileSystem.listFiles(path);
            nbFiles = files.size();
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (nbRetry > maxNbRetries) {
            Assertions.fail("Wrong number of files generated : " + nbFiles + " instead of " + expectedFiles);
        }

        List<String> csvLines = new ArrayList<>();
        files.stream().map(FileEntry::createInputStream).map(is -> {
            try (ByteArrayOutputStream bout = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[1024];
                int read = 0;
                while ((read = is.read(buffer)) > 0) {
                    bout.write(buffer, 0, read);
                    bout.flush();
                }
                return bout.toByteArray();
            } catch (IOException ioe) {
                log.error(ioe.getMessage(), ioe);
                return new byte[0];
            }
        }).map(b -> new String(b, StandardCharsets.ISO_8859_1)).forEach(s -> {
            Arrays.stream(s.split("\r\n")).filter(l -> !"".equals(l.trim())).forEach(csvLines::add);
        });

        Assertions.assertEquals(nbRecords, csvLines.size(), "Wrong number of lines");
        files.stream().map(FileEntry::getName).forEach(fileSystem::delete);
    }
}
