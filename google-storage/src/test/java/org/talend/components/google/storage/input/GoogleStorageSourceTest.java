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
package org.talend.components.google.storage.input;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.format.LineConfiguration.LineSeparatorType;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator;
import org.talend.components.google.storage.dataset.FormatConfiguration;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.CredentialService;
import org.talend.components.google.storage.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

@WithComponents(value = "org.talend.components.google.storage")
class GoogleStorageSourceTest {

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Service
    private I18nMessage i18n;

    private final Storage storage = LocalStorageHelper.getOptions().getService();

    @Test
    void next() throws IOException {
        BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of("bucketTest", "blob/path")).build();

        String content = "C1;C2\nL1;L2\nM1;M2\nP1;P2";

        storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));

        GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(new GSDataStore());
        String jwtContent = this.getContentFile("./engineering-test.json");
        dataset.getDataStore().setJsonCredentials(jwtContent);
        dataset.setBucket("bucketTest");
        dataset.setBlob("blob/path");

        FormatConfiguration configuration = new FormatConfiguration();
        configuration.setContentFormat(FormatConfiguration.Type.CSV);
        CSVConfiguration csvConfig = new CSVConfiguration();
        csvConfig.setFieldSeparator(new FieldSeparator());
        csvConfig.getFieldSeparator().setFieldSeparatorType(FieldSeparator.Type.SEMICOLON);
        csvConfig.setQuotedValue('"');
        csvConfig.setEscape('\\');

        LineConfiguration lineCfg = new LineConfiguration();
        lineCfg.setLineSeparatorType(LineSeparatorType.LF);
        csvConfig.setLineConfiguration(lineCfg);
        configuration.setCsvConfiguration(csvConfig);
        dataset.setContentFormat(configuration);

        final InputConfiguration config = new InputConfiguration();
        config.setDataset(dataset);

        final CredentialService credentialService = new CredentialService() {

            @Override
            public Storage newStorage(GoogleCredentials credentials) {
                return GoogleStorageSourceTest.this.storage;
            }
        };

        GoogleStorageSource source = new GoogleStorageSource(config, this.factory, this.repository, credentialService, i18n);

        source.init();

        final Record record1 = source.next();
        this.checkRecord(record1, "C1", "C2");

        final Record record2 = source.next();
        this.checkRecord(record2, "L1", "L2");

        final Record record3 = source.next();
        this.checkRecord(record3, "M1", "M2");

        final Record record4 = source.next();
        this.checkRecord(record4, "P1", "P2");

        final Record record5 = source.next();
        Assertions.assertNull(record5);

        source.release();
    }

    private void checkRecord(Record record, String col1, String col2) {
        Assertions.assertNotNull(record);
        Assertions.assertEquals(col1, record.getString("field_1"));
        Assertions.assertEquals(col2, record.getString("field_2"));
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);

        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }

}