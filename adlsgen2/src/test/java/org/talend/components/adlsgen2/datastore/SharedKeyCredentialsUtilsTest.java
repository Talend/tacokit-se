/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
import java.security.InvalidKeyException;
import java.util.HashMap;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.junit5.WithComponents;

import com.microsoft.rest.v2.http.HttpHeaders;
import com.microsoft.rest.v2.http.HttpMethod;
import com.microsoft.rest.v2.http.HttpRequest;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WithComponents("org.talend.components.adlsgen2")
class SharedKeyCredentialsUtilsTest extends AdlsGen2TestBase {

    private SharedKeyUtils utils;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        try {
            utils = new SharedKeyUtils(accountName, accountKey);
        } catch (InvalidKeyException e) {
            throw new IllegalStateException(e);
        }
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void getAccountName() {
        assertEquals(accountName, utils.getAccountName());
    }

    @Test
    void buildAuthenticationSignature() throws Exception {
        URL url = new URL(
                "https://undxgen2.dfs.core.windows.net/adls-gen2?directory=myNewFolder&recursive=false&resource=filesystem&timeout=60");
        HttpHeaders headers = new HttpHeaders(new HashMap<>());
        HttpRequest request = new HttpRequest(null, HttpMethod.GET, url, headers, null, null);
        String signature = utils.buildAuthenticationSignature(request);
        assertNotNull(signature);
        assertTrue(signature.startsWith("SharedKey undxgen2:"));
    }
}
