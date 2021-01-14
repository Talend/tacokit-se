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
package org.talend.components.google.storage.input;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator.Type;
import org.talend.components.google.storage.GSServiceFake;
import org.talend.components.google.storage.dataset.FormatConfiguration;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.GSService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.google.storage")
class GoogleStorageSourceTest {

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Service
    private I18nMessage i18n;

    @Service
    private GSService services;

    @Test
    void source() throws IOException {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./bucketSource");
        final GSService fake = new GSServiceFake(this.services, new File(resource.getPath()), "test");

        final GSDataStore dataStore = this.buildDataStore();
        GSDataSet dataset = new GSDataSet();
        dataset.setBucket("test");
        dataset.setBlob("blob");
        dataset.setDataStore(dataStore);

        final FormatConfiguration configuration = new FormatConfiguration();
        configuration.setContentFormat(FormatConfiguration.Type.CSV);
        CSVConfiguration csvConfig = new CSVConfiguration();
        csvConfig.getFieldSeparator().setFieldSeparatorType(Type.COMMA);
        csvConfig.setQuotedValue('"');
        csvConfig.setEscape('\\');

        dataset.setContentFormat(configuration);
        final InputConfiguration config = new InputConfiguration();
        config.setDataset(dataset);

        final GoogleStorageSource source = new GoogleStorageSource(config, this.factory, this.repository, fake);
        Record record = source.next();
        int count = 0;
        while (record != null) {
            final String f1 = record.getString("field_1");
            Assertions.assertEquals('b', f1.charAt(0), "explore wrong file");

            record = source.next();
            count++;
        }
        Assertions.assertEquals(6, count);

    }

    private GSDataStore buildDataStore() throws IOException {
        final GSDataStore ds = new GSDataStore();
        String jwtContent = this.getContentFile("./engineering-test.json");
        ds.setJsonCredentials(jwtContent);
        return ds;
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);

        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }

}