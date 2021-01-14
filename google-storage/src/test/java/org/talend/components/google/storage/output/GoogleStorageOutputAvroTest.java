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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.components.google.storage.GSServiceFake;
import org.talend.components.google.storage.dataset.FormatConfiguration;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.google.storage")
public class GoogleStorageOutputAvroTest {

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

        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./bucketAvro");
        final File root = new File(resource.getPath());
        this.services = new GSServiceFake(services, root, "bucketTest");

        final GSDataSet dataset = this.prepareDataset();

        dataset.setBucket("bucketTest");
        dataset.setBlob("blobavro");

        final GoogleStorageOutput output = this.buildOutput(dataset);

        output.init();
        final Collection<Record> records = buildRecords();
        output.write(records);
        output.release();

        Stream<File> avroFiles = Stream.of(root.listFiles()).filter((File f) -> f.getName().startsWith("blobavro"));

        final Optional<File> first = avroFiles.findFirst();
        Assertions.assertTrue(first.isPresent());

        try (final InputStream in = new FileInputStream(first.get());
                final BufferedReader inputStream = new BufferedReader(new InputStreamReader(in))) {
            final String collect = inputStream.lines().collect(Collectors.joining("\n"));
            Assertions.assertNotNull(collect);
            Assertions.assertTrue(collect.startsWith("Obj"));
        }
    }

    private GoogleStorageOutput buildOutput(final GSDataSet dataset) {
        final OutputConfiguration config = new OutputConfiguration();
        config.setDataset(dataset);

        return new GoogleStorageOutput(config, this.repository, i18n, this.services);
    }

    private GSDataSet prepareDataset() throws IOException {
        final GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(new GSDataStore());

        final FormatConfiguration format = new FormatConfiguration();
        dataset.setContentFormat(format);
        format.setContentFormat(FormatConfiguration.Type.AVRO);
        format.setAvroConfiguration(new AvroConfiguration());

        final String jwtContent = this.getContentFile("./engineering-test.json");
        dataset.getDataStore().setJsonCredentials(jwtContent);

        return dataset;
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
