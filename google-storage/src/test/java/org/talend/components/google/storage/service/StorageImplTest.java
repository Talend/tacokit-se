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
package org.talend.components.google.storage.service;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.google.storage.CredentialServiceFake;
import org.talend.components.google.storage.FakeStorage;
import org.talend.components.google.storage.dataset.GSDataSet;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.google.storage")
class StorageImplTest {

    private final Storage storage = FakeStorage.buildForTU();

    @Service
    private I18nMessage i18n;

    @Service
    private CredentialService credentialService;

    @Service
    private GSService services;

    @BeforeEach
    void init() {
        this.credentialService = new CredentialServiceFake(this.storage, this.credentialService);
    }

    @Test
    void buildInput() throws IOException {
        final String content = "C1;C2\nL1;L2\nM1;M2\nP1;P2";
        final GSDataSet dataSet = storeContent("bucket", "blob", content);

        StorageImpl st = new StorageImpl(this.credentialService, dataSet.getDataStore().getJsonCredentials(), i18n);
        final Supplier<InputStream> inputGetter = st.buildInput("bucket", "blob");
        Assertions.assertNotNull(inputGetter);
        try (InputStream input = inputGetter.get()) {
            Assertions.assertNotNull(input);
            byte[] buf = new byte[200];
            final int size = input.read(buf);
            Assertions.assertEquals(content, new String(buf, 0, size));
        }

        try {
            final Supplier<InputStream> inputUnexist = st.buildInput("bucket", "unknown");
            Assertions.fail("unknown should not exist");
        } catch (ComponentException ex) {
        }

        try {
            final Supplier<InputStream> inputUnexist2 = st.buildInput("unknown", "unknown");
            Assertions.fail("unknown should not exist");
        } catch (ComponentException ex) {
        }

    }

    @Test
    void buildOutput() throws IOException {
        final GSDataStore dataStore = buildDataStore();
        StorageImpl st = new StorageImpl(this.credentialService, dataStore.getJsonCredentials(), i18n);
        try (final OutputStream outputStream = st.buildOuput("output", "blob")) {
            outputStream.write("Hello".getBytes());
        }

        final Supplier<InputStream> inputGetter = st.buildInput("output", "blob");
        try (InputStream input = inputGetter.get()) {
            Assertions.assertNotNull(input);
            byte[] buf = new byte[200];
            final int size = input.read(buf);
            Assertions.assertEquals("Hello", new String(buf, 0, size));
        }
    }

    @Test
    void findBlobsName() throws IOException {
        final GSDataStore dataStore = buildDataStore();
        final StorageImpl st = new StorageImpl(this.credentialService, dataStore.getJsonCredentials(), i18n);

        final BlobNameBuilder builder = new BlobNameBuilder();
        final String b1 = builder.generateName("blob");
        final String b2 = builder.generateName("blob");
        final String b3 = builder.generateName("blob");

        this.storeContent("bucket", b1, "Hello");
        this.storeContent("bucket", b2, "Bonjour");
        this.storeContent("bucket", b3, "Pryvit");

        final List<String> blobsName = st.findBlobsName("bucket", "blob") //
                .collect(Collectors.toList());
        Assertions.assertEquals(3, blobsName.size());
        blobsName.stream().allMatch((String n) -> builder.isGenerated("blob", n));
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);

        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }

    private GSDataSet storeContent(final String bucketName, final String blobName, final String content) throws IOException {
        final BlobInfo blobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, blobName)).build();
        this.storage.create(blobInfo, content.getBytes(StandardCharsets.UTF_8));

        final GSDataSet dataset = new GSDataSet();
        dataset.setDataStore(this.buildDataStore());
        dataset.setBucket(bucketName);
        dataset.setBlob(blobName);
        return dataset;
    }

    private GSDataStore buildDataStore() throws IOException {
        final GSDataStore ds = new GSDataStore();
        String jwtContent = this.getContentFile("./engineering-test.json");
        ds.setJsonCredentials(jwtContent);
        return ds;
    }
}