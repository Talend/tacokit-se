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
package org.talend.components.google.storage.output;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.cloud.storage.Storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.google.storage.FakeStorage;
import org.talend.components.google.storage.GSServiceFake;
import org.talend.components.google.storage.dataset.FormatConfiguration;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.dataset.JsonAllConfiguration;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.google.storage")
class GoogleStorageOutputTest {

    private final Storage storage = FakeStorage.buildForTU();

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Service
    private I18nMessage i18n;

    @Service
    private GSService services;

    @Test
    void write() throws IOException {

        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./bucketTarget");
        final File target = new File(resource.getPath());

        // Prepare test target folder
        final File[] files = target.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.exists() && f.isFile() && !("dummy.txt".equals(f.getName()))) {
                    f.delete();
                }
            }
        }
        final GSService fake = new GSServiceFake(this.services, target, "test");

        final GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(new GSDataStore());

        final FormatConfiguration format = dataset.getContentFormat();
        format.setContentFormat(FormatConfiguration.Type.JSON);
        format.setJsonConfiguration(new JsonAllConfiguration());

        final String jwtContent = this.getContentFile("./engineering-test.json");
        dataset.getDataStore().setJsonCredentials(jwtContent);
        dataset.setBucket("test");
        dataset.setBlob("blob");

        final OutputConfiguration config = new OutputConfiguration();
        config.setDataset(dataset);
        final GoogleStorageOutput output = new GoogleStorageOutput(config, this.repository, i18n, fake);

        output.init();
        final Collection<Record> records = buildRecords();

        output.write(records);
        output.release();

        final File[] filesRes = target.listFiles();
        final Optional<File> first = Stream.of(filesRes) //
                .filter((File f) -> f.getName().startsWith("blob")) //
                .findFirst(); //
        Assertions.assertTrue(first.isPresent());
    }

    private Collection<Record> buildRecords() {
        final Record record1 = factory.newRecordBuilder().withString("Hello", "World")
                .withRecord("sub1", factory.newRecordBuilder().withInt("max", 200).withBoolean("uniq", false).build()).build();

        final Record record2 = factory.newRecordBuilder().withString("xx", "zz")
                .withArray(
                        factory.newEntryBuilder().withName("array").withType(Schema.Type.ARRAY)
                                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()).build(),
                        Arrays.asList("v1", "v2"))
                .build();

        final Record record3 = factory.newRecordBuilder().withLong("val1", 234L).build();
        return Arrays.asList(record1, record2, record3);
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);

        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }
}