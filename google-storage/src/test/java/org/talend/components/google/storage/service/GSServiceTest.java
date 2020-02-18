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
package org.talend.components.google.storage.service;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.google.storage")
class GSServiceTest {

    @Service
    private GSService service;

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

        final Item firstItem = blobsName.getItems().iterator().next();
        Assertions.assertEquals("rep/first.txt", firstItem.getId());
    }

    private String getContentFile(String relativePath) throws IOException {
        final URL urlJWT = Thread.currentThread().getContextClassLoader().getResource(relativePath);
        final File ficJWT = new File(urlJWT.getPath());
        return new String(Files.readAllBytes(ficJWT.toPath()));
    }
}