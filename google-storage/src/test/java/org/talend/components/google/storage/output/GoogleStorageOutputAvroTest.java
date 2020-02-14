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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.Messages;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.common.stream.format.FormatConfiguration;
import org.talend.components.common.stream.format.FormatConfiguration.Type;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.CredentialService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

@WithComponents(value = "org.talend.components.google.storage")
public class GoogleStorageOutputAvroTest {

    private final Storage storage = LocalStorageHelper.getOptions().getService();

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Injected
    private BaseComponentsHandler handler;

    @Service
    private Messages messages;

    @BeforeEach
    void buildConfig() throws IOException {
        // Inject needed services
        handler.injectServices(this);

    }

    @Test
    void write() throws IOException {
        GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(new GSDataStore());

        final FormatConfiguration format = new FormatConfiguration();
        dataset.setContentFormat(format);
        format.setContentFormat(Type.AVRO);
        format.setAvroConfiguration(new AvroConfiguration());

        final String jwtContent = this.getContentFile("./engineering-test.json");
        dataset.getDataStore().setJsonCredentials(jwtContent);
        dataset.setBucket("bucketTest");
        dataset.setBlob("blob/avropath");

        final OutputConfiguration config = new OutputConfiguration();
        config.setDataset(dataset);
        CredentialService credService = new CredentialService() {

            @Override
            public Storage newStorage(GoogleCredentials credentials) {
                return GoogleStorageOutputAvroTest.this.storage;
            }
        };
        final GoogleStorageOutput output = new GoogleStorageOutput(config, //
                credService, //
                this.repository);

        output.init();
        final Collection<Record> records = buildRecords();
        output.write(records);
        output.release();
    }

    private Collection<Record> buildRecords() {
        final Record record1 = factory.newRecordBuilder() //
                .withString("Hello", "World") //
                .withRecord("sub1", factory.newRecordBuilder() //
                        .withInt("max", 200) //
                        .withBoolean("uniq", false) //
                        .build()) //
                .withArray( //
                        factory.newEntryBuilder().withName("array") //
                                .withType(Schema.Type.ARRAY) //
                                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()) //
                                .build(), //
                        Arrays.asList("v1", "v2")) //
                .build(); //

        final Record record2 = factory.newRecordBuilder() //
                .withString("Hello", "You") //
                .withRecord("sub1", factory.newRecordBuilder() //
                        .withInt("max", 100) //
                        .withBoolean("uniq", true) //
                        .build()) //
                .withArray( //
                        factory.newEntryBuilder().withName("array") //
                                .withType(Schema.Type.ARRAY) //
                                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()) //
                                .build(), //
                        Arrays.asList("a1", "a2")) //
                .build(); //

        final Record record3 = factory.newRecordBuilder() //
                .withString("Hello", "ss") //
                .withRecord("sub1", factory.newRecordBuilder() //
                        .withInt("max", 50) //
                        .withBoolean("uniq", false) //
                        .build()) //
                .withArray( //
                        factory.newEntryBuilder().withName("array") //
                                .withType(Schema.Type.ARRAY) //
                                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()) //
                                .build(), //
                        Arrays.asList("v1", "455", "dze")) //
                .build(); //
        return Arrays.asList(record1, record2, record3);
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);

        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }
}
