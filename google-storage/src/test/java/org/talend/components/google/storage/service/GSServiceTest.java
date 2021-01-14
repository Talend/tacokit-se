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
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Collection;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.google.storage.StorageFacadeFake;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.sdk.component.api.exception.ComponentException;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.google.storage")
class GSServiceTest {

    private final Storage storage = LocalStorageHelper.getOptions().getService();

    @Service
    private GSService service;

    @Service
    private RecordIORepository iorepo;

    @Service
    private I18nMessage i18n;

    @Service
    private CredentialService credentialService;

    @Test
    void healthCheck() throws IOException {
        final GSDataStore ds = new GSDataStore();
        final HealthCheckStatus statusKO = service.healthCheck(ds);
        Assertions.assertSame(HealthCheckStatus.Status.KO, statusKO.getStatus());

        String jwtContentKO = this.getContentFile("./engineering-test_ERROR.json");
        ds.setJsonCredentials(jwtContentKO);
        final HealthCheckStatus statusKO2 = service.healthCheck(ds);
        Assertions.assertSame(HealthCheckStatus.Status.KO, statusKO2.getStatus());

        String jwtContent = this.getContentFile("./engineering-test.json");
        ds.setJsonCredentials(jwtContent);

        final HealthCheckStatus status = service.healthCheck(ds);
        Assertions.assertSame(HealthCheckStatus.Status.OK, status.getStatus(), () -> "Not OK : " + status.getComment());
    }

    @Test
    void findBucketsName() throws IOException {
        final GSDataStore ds = new GSDataStore();
        String jwtContent = this.getContentFile("./engineering-test.json");
        ds.setJsonCredentials(jwtContent);

        final SuggestionValues bucketsName = this.service.findBucketsName(ds);
        Assertions.assertNotNull(bucketsName);
        Assertions.assertEquals(125, bucketsName.getItems().size());
        final Item firstItem = bucketsName.getItems().iterator().next();
        Assertions.assertEquals("bucket1", firstItem.getId());
    }

    @Test
    void findBlobsName() throws IOException {
        final GSDataStore ds = new GSDataStore();
        String jwtContent = this.getContentFile("./engineering-test.json");
        ds.setJsonCredentials(jwtContent);

        final SuggestionValues blobsName = this.service.findBlobsName(ds, "mybucket");
        Assertions.assertNotNull(blobsName);

        final Collection<Item> items = blobsName.getItems();
        Assertions.assertEquals(1, items.stream().filter((Item it) -> "blob".equals(it.getId())).count());
        Assertions.assertEquals(1,
                items.stream().filter((Item it) -> "blob_689d651a-5896-428d-9816-2de624d0046a".equals(it.getId())).count());
        final Item firstItem = items.iterator().next();
        Assertions.assertEquals("rep/first.txt", firstItem.getId());
    }

    @Test
    void checkBucket() {
        StorageFacade storage = new StorageFacadeFake("bucket", new File("."));
        this.service.checkBucket(storage, "bucket");

        try {
            this.service.checkBucket(storage, "unknown");
            Assertions.fail("should have thrown exception");
        } catch (ComponentException ex) {

        }
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);
        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()), Charset.defaultCharset());
    }
}