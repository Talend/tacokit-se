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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.common.Constants;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

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

    @Test
    void testSerial() throws IOException, ClassNotFoundException {
        this.connection.setAuthMethod(AuthMethod.ActiveDirectory);
        this.connection.setSas("sas");
        this.connection.setClientId("clientId");
        this.connection.setClientSecret("clientSecret");
        this.connection.setTenantId("tenant");
        this.connection.setTimeout(200);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(this.connection);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final AdlsGen2Connection cnxCopy = (AdlsGen2Connection) ois.readObject();
        Assertions.assertEquals(this.connection, cnxCopy);

    }
}
