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
package org.talend.components.common.service.http.digest;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.service.http.Configurer;

import java.util.Optional;

@Slf4j
public class DigestAuthConfigurer implements Configurer {

    private final static String AUTHORIZATION_HEADER = System
            .getProperty("org.talend.components.common.service.http.digest.authorization_header", "Authorization");

    public final static String DIGEST_CONTEXT_CONF = "digestContext";

    @Override
    public void configure(Connection connection, ConfigurerConfiguration configuration) {
        log.debug("Configure digest authentication");
        DigestAuthContext context = configuration.get(DIGEST_CONTEXT_CONF, DigestAuthContext.class);

        Optional<String> digestAuthHeader = Optional.ofNullable(context.getDigestAuthHeader());
        digestAuthHeader.ifPresent(v -> {
            log.debug("Set Authorization header with digest");
            connection.withHeader(AUTHORIZATION_HEADER, v);
        });
    }

}
