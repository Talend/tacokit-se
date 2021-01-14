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
package org.talend.components.adlsgen2.datastore;

import java.net.URL;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnJre;
import org.junit.jupiter.api.condition.JRE;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.datastore.Constants.MethodConstants;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.adlsgen2")
class SharedKeyCredentialsUtilsTest extends AdlsGen2TestBase {

    public static final String USERNAME = "username";

    private SharedKeyUtils utils;

    private URL url;

    private Map<String, String> headers;

    private String signature;

    private String expectedSignature;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        utils = new SharedKeyUtils(USERNAME, accountKey);
        url = new URL(
                "https://undxgen2.dfs.core.windows.net/adls-gen2?directory=myNewFolder&recursive=false&resource=filesystem&timeout=60");
        headers = new HashMap<>();
        OffsetDateTime signatureTime = OffsetDateTime.of(2019, 8, 7, 11, 11, 0, 0, ZoneOffset.UTC);
        headers.put(Constants.HeaderConstants.USER_AGENT, Constants.HeaderConstants.USER_AGENT_AZURE_DLS_GEN2);
        headers.put(Constants.HeaderConstants.DATE, Constants.RFC1123GMTDateFormatter.format(signatureTime));
        headers.put(Constants.HeaderConstants.CONTENT_TYPE, Constants.HeaderConstants.DFS_CONTENT_TYPE);
        headers.put(Constants.HeaderConstants.VERSION, Constants.HeaderConstants.TARGET_STORAGE_VERSION);
    }

    @Test
    void getAccountName() {
        assertEquals(USERNAME, utils.getAccountName());
    }

    @Test
    @EnabledOnJre({ JRE.JAVA_11 })
    void checkSignature() throws Exception {
        expectedSignature = "SharedKey username:8GgdJcEk+FLk2oNJeLuAAIj4O2QMrG4Z1SE4xoV3oTI=";
        signature = utils.buildAuthenticationSignature(url, MethodConstants.GET, headers);
        assertNotNull(signature);
        assertTrue(signature.startsWith("SharedKey username:"));
        assertEquals(expectedSignature, signature);
        OffsetDateTime oneSecLater = OffsetDateTime.of(2019, 8, 7, 11, 11, 1, 0, ZoneOffset.UTC);
        headers.put(Constants.HeaderConstants.DATE, Constants.RFC1123GMTDateFormatter.format(oneSecLater));
        signature = utils.buildAuthenticationSignature(url, MethodConstants.GET, headers);
        assertNotEquals(expectedSignature, signature);
    }

    @Test
    @EnabledOnJre({ JRE.JAVA_11 })
    void checkSignatureWithSpacesInBlobName() throws Exception {
        expectedSignature = "SharedKey username:jIgUXkXGQGVmfyHYMWDC7AGEkQyVmcldGmTiQYL4oFc=";
        url = new URL("https://undxgen2.dfs.core.windows.net/adls-gen2/directory/my New Blob.spacy&timeout=60");
        signature = utils.buildAuthenticationSignature(url, MethodConstants.GET, headers);
        assertNotNull(signature);
        assertTrue(signature.startsWith("SharedKey username:"));
        assertEquals(expectedSignature, signature);
        url = new URL("https://undxgen2.dfs.core.windows.net/adls-gen2/directory/my%20New%20Blob.spacy&timeout=60");
        signature = utils.buildAuthenticationSignature(url, MethodConstants.GET, headers);
        assertNotNull(signature);
        assertTrue(signature.startsWith("SharedKey username:"));
        assertEquals(expectedSignature, signature);
    }
}
