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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AdlsGen2ConnectionTest {

    public static final String CHINA_STORAGE_ACCOUNT = "datalakegen2";

    public static final String CHINA_ENDPOINT_SUFFIX = "core.chinacloudapi.cn";

    public static final String CHINA_API_URL = "https://datalakegen2.core.chinacloudapi.cn";

    public static final String UNDXGEN2_STORAGE_ACCOUNT = "undxgen2";

    public static final String UNDXGEN2_API_URL = "https://undxgen2.dfs.core.windows.net";

    private AdlsGen2Connection connection;

    @BeforeEach
    void setUp() {
        connection = new AdlsGen2Connection();
    }

    @Test
    void getDefaultApiUrl() {
        connection.setAccountName(UNDXGEN2_STORAGE_ACCOUNT);
        assertEquals(UNDXGEN2_API_URL, connection.apiUrl());
    }

    @Test
    void getChinaApiUrl() {
        connection.setAccountName(CHINA_STORAGE_ACCOUNT);
        connection.setEndpointSuffix(CHINA_ENDPOINT_SUFFIX);
        assertEquals(CHINA_API_URL, connection.apiUrl());
    }

    @Test
    void getDefaultEndpointSuffix() {
        assertEquals(Constants.DFS_DEFAULT_ENDPOINT_SUFFIX, connection.getEndpointSuffix());
    }

    @Test
    void getChinaEndpointSuffix() {
        connection.setEndpointSuffix(CHINA_ENDPOINT_SUFFIX);
        assertEquals(CHINA_ENDPOINT_SUFFIX, connection.getEndpointSuffix());
    }
}
