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
package org.talend.components.google.storage.output;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.google.storage.dataset.FormatConfiguration;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.dataset.JsonAllConfiguration;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.CredentialService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

@WithComponents(value = "org.talend.components.google.storage")
class GoogleStorageOutputTest {

    private final Storage storage = LocalStorageHelper.getOptions().getService();

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Service
    private I18nMessage i18n;

    @Test
    void write() throws IOException {
        GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(new GSDataStore());

        final FormatConfiguration format = new FormatConfiguration();
        dataset.setContentFormat(format);
        format.setContentFormat(FormatConfiguration.Type.JSON);
        format.setJsonConfiguration(new JsonAllConfiguration());

        final String jwtContent = this.getContentFile("./engineering-test.json");
        dataset.getDataStore().setJsonCredentials(jwtContent);
        dataset.setBucket("bucketTest");
        dataset.setBlob("blob/path");

        final OutputConfiguration config = new OutputConfiguration();
        config.setDataset(dataset);
        final CredentialService credService = new CredentialService() {

            @Override
            public Storage newStorage(GoogleCredentials credentials) {
                return GoogleStorageOutputTest.this.storage;
            }
        };
        final GoogleStorageOutput output = new GoogleStorageOutput(config, credService, this.repository, i18n);

        output.init();
        final Collection<Record> records = buildRecords();

        output.write(records);
        output.release();

        final Blob blob = storage.get(BlobId.of("bucketTest", "blob/path"));
        try (final ReadChannel reader = blob.reader();
                final InputStream in = Channels.newInputStream(reader);
                final BufferedReader inputStream = new BufferedReader(new InputStreamReader(in))) {
            final String collect = inputStream.lines().collect(Collectors.joining("\n"));
            Assertions.assertNotNull(collect);
            Assertions.assertTrue(collect.startsWith("["));
        }
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