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
package org.talend.component.common.service.http;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.service.http.Configurer;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

@Slf4j
public class DigestAuthConfigurer implements Configurer {

    @Override
    public void configure(Connection connection, ConfigurerConfiguration configuration) {
        try {
            URI uri = new URI(connection.getUrl());
            DigestAuthContext context = Optional.ofNullable(DigestAuthContext.getThreadLocalContext())
                    .orElse(new DigestAuthContext(uri.getPath(), connection.getMethod(), uri.getHost(), uri.getPort(),
                            connection.getPayload()));

            DigestAuthContext.setThreadLocalContext(context);

            Optional<String> digestAuthHeader = Optional.ofNullable(context.getDigestAuthHeader());
            digestAuthHeader.ifPresent(v -> {
                log.debug("Set Authorization header with digest");
                connection.withHeader("Authorization", v);
            });
        } catch (URISyntaxException e) {
            log.error("Given url is malformed '" + connection.getUrl() + "'", e);
        }
    }

}
