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
package org.talend.components.adlsgen2.service;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.ClientGen2Fake;
import org.talend.components.adlsgen2.FakeActiveDirectoryService;
import org.talend.components.adlsgen2.FakeResponse;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.components.adlsgen2.runtime.AdlsDatasetRuntimeInfo;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.ComponentManager;

@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2ServiceTest extends AdlsGen2TestBase {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Test
    void handleResponse() {
        final Response<String> respOK = new FakeResponse<>(200, "OK", Collections.emptyMap(), "");
        Assertions.assertSame(respOK, AdlsGen2Service.handleResponse(respOK));

        final Map<String, List<String>> headers = new HashMap<>();
        headers.put(HeaderConstants.HEADER_X_MS_ERROR_CODE,
                Arrays.asList("The specified account is disabled.", "One of the request inputs is not valid."));
        try {
            final Response<String> respKO = new FakeResponse<>(500, "OK", headers, "");
            AdlsGen2Service.handleResponse(respKO);
            Assertions.fail("not on exception");
        } catch (AdlsGen2RuntimeException ex) {
            Assertions.assertTrue(ex.getMessage().contains("The specified account is disabled"), ex.getMessage());
        }
    }

    @Test
    void testgetBlobs() {
        final JsonObject filesystems = Json.createObjectBuilder()
                .add("paths", Json.createArrayBuilder().add(Json.createObjectBuilder() //
                        .add("etag", "0x8D89D1980D8BD4B") //
                        .add("name", "/paht1/file1.txt") //
                        .add("contentLength", "120") //
                        .add("lastModified", "2021-01-13") //
                        .add("owner", "admin") //
                        .build()) //
                        .build())
                .build();

        final AdlsGen2Service service = this.getServiceForJson(filesystems);
        AdlsDatasetRuntimeInfo runtimeInfo = new AdlsDatasetRuntimeInfo(this.dataSet, new FakeActiveDirectoryService());
        final List<BlobInformations> blobs = service.getBlobs(runtimeInfo);
        Assertions.assertEquals(1, blobs.size());

        final BlobInformations informations = blobs.get(0);
        Assertions.assertEquals(120, informations.getContentLength());
        Assertions.assertEquals("0x8D89D1980D8BD4B", informations.getEtag());
        Assertions.assertEquals("/paht1", informations.getDirectory());
        Assertions.assertEquals("file1.txt", informations.getFileName());
        Assertions.assertNull(informations.getPermissions());
        Assertions.assertEquals("admin", informations.getOwner());
    }

    @Test
    void testGetBlobInformations() {
        final JsonObject filesystems = Json.createObjectBuilder()
                .add("paths", Json.createArrayBuilder().add(Json.createObjectBuilder() //
                        .add("etag", "0x8D89D1980D8BD4B") //
                        .add("name", "/paht1/file1.txt") //
                        .add("contentLength", "120") //
                        .add("lastModified", "2021-01-13") //
                        .add("owner", "admin") //
                        .add("permissions", "read") //
                        .build()) //
                        .build())
                .build();
        final AdlsGen2Service service = this.getServiceForJson(filesystems);
        this.dataSet.setBlobPath("/paht1/file1.txt");
        final AdlsDatasetRuntimeInfo runtimeInfo = new AdlsDatasetRuntimeInfo(this.dataSet, new FakeActiveDirectoryService());
        final BlobInformations informations = service.getBlobInformations(runtimeInfo);
        Assertions.assertEquals(120, informations.getContentLength());
        Assertions.assertEquals("0x8D89D1980D8BD4B", informations.getEtag());
        Assertions.assertEquals("file1.txt", informations.getFileName());
        Assertions.assertEquals("read", informations.getPermissions());
        Assertions.assertEquals("admin", informations.getOwner());

        Assertions.assertTrue(service.blobExists(runtimeInfo, "/paht1/file1.txt"));
    }

    private AdlsGen2Service getServiceForJson(JsonObject expectedResult) {
        final ComponentManager manager = componentsHandler.asManager();

        ClientGen2Fake fake = new ClientGen2Fake(new FakeResponse<>(200, expectedResult, null, null));
        ClientGen2Fake.inject(manager, fake);
        return this.componentsHandler.findService(AdlsGen2Service.class);
    }
}