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

import org.talend.components.adlsgen2.datastore.AdlsGen2Connection;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.sdk.component.api.service.http.Configurer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccessTokenConfigurer implements Configurer {

    protected static final String CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED = "application/x-www-form-urlencoded";

    @Override
    public void configure(Connection connection, ConfigurerConfiguration configuration) {
        final AdlsGen2Connection conn = configuration.get("connection", AdlsGen2Connection.class);
        log.debug("[configure] [{}] {}", connection.getMethod(), connection.getUrl());
        connection //
                .withHeader(HeaderConstants.CONTENT_TYPE, CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED) //
                .withHeader(HeaderConstants.CONTENT_LENGTH, String.valueOf(connection.getPayload().length));
    }
}
