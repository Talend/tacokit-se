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
package org.talend.components.adlsgen2.service;

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.sdk.component.api.service.http.Configurer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AdlsGen2APIConfigurer implements Configurer {

    @Override
    public void configure(final Connection connection, final ConfigurerConfiguration configuration) {
        final AdlsGen2Connection conn = configuration.get("connection", AdlsGen2Connection.class);
        final String auth = configuration.get("auth", String.class);
        if (!AuthMethod.SAS.equals(conn.getAuthMethod())) {
            connection.withHeader(HeaderConstants.AUTHORIZATION, auth);
        }
        if (connection.getMethod().equals("POST")) {
            connection.withHeader(" X-HTTP-Method", "PUT"); //
        }
    }
}
