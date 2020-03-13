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
package org.talend.components.adlsgen2.common.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.adlsgen2.common.connection.AdlsGen2Connection;
import org.talend.sdk.component.api.service.http.Configurer;

import static org.talend.components.adlsgen2.common.connection.Constants.HeaderConstants.*;

@Slf4j
public class AdlsGen2APIConfigurer implements Configurer {

    @Override
    public void configure(final Connection connection, final ConfigurerConfiguration configuration) {
        final AdlsGen2Connection conn = configuration.get("connection", AdlsGen2Connection.class);
        final String auth = configuration.get("auth", String.class);
        final String date = configuration.get("date", String.class);
        final String filePath = configuration.get("file", String.class);
        log.debug("[configure] connection {}", conn);
        log.warn("[configure] [{}] {}", connection.getMethod(), connection.getUrl());
        log.debug("[configure] auth       {}", auth);

        connection.withHeader(CONTENT_TYPE, DFS_CONTENT_TYPE) //
                .withHeader(VERSION, TARGET_STORAGE_VERSION)
        // .withHeader("x-ms-blob-type", "BlockBlob")
        ;
    }
}
