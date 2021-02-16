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
package org.talend.components.common.service.http.basic;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.service.http.common.UserNamePassword;
import org.talend.sdk.component.api.service.http.Configurer;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
public class BasicAuthConfigurer implements Configurer {

    public final static String BASIC_CONTEXT_CONF = "basicContext";

    @Override
    public void configure(Connection connection, ConfigurerConfiguration configuration) {
        log.debug("Configure basic authentication");
        UserNamePassword context = configuration.get(BASIC_CONTEXT_CONF, UserNamePassword.class);
        connection.withHeader("Authorization", "Basic " + Base64.getEncoder()
                .encodeToString((context.getUser() + ":" + context.getPassword()).getBytes(StandardCharsets.UTF_8)));
    }

}
