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
package org.talend.components.common.service.http.bearer;

import lombok.extern.slf4j.Slf4j;
import org.talend.sdk.component.api.service.http.Configurer;

@Slf4j
public class BearerAuthConfigurer implements Configurer {

    public final static String BEARER_TOKEN_CONF = "bearerToken";

    @Override
    public void configure(Connection connection, ConfigurerConfiguration configuration) {
        log.debug("Configure bearer authentication");
        String context = configuration.get(BEARER_TOKEN_CONF, String.class);
        connection.withHeader("Authorization", "Bearer " + context);
    }

}
