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
package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.common.service.http.basic.BasicAuthConfigurer;
import org.talend.components.common.service.http.bearer.BearerAuthConfigurer;
import org.talend.components.common.service.http.digest.DigestAuthConfigurer;
import org.talend.components.rest.configuration.Param;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.service.http.Configurer;

@Slf4j
public class RestConfigurer implements Configurer {

    @Override
    public void configure(final Connection connection, final ConfigurerConfiguration configuration) {
        final RequestConfig config = configuration.get("configuration", RequestConfig.class);
        final Client client = configuration.get("httpClient", Client.class);

        if (config.getDataset().getDatastore().getConnectionTimeout() != null) {
            connection.withConnectionTimeout(config.getDataset().getDatastore().getConnectionTimeout());
        }
        if (config.getDataset().getDatastore().getReadTimeout() != null) {
            connection.withReadTimeout(config.getDataset().getDatastore().getReadTimeout());
        }

        // Add Content-Type if body and add default encoding if none.
        byte[] body = config.getDataset().getBody().getType().getBytes(config.getDataset().getBody());
        if (body.length > 0) {
            if (config.getDataset().getHasHeaders()) {
                String contentType = config.getDataset().getHeaders().stream()
                        .filter(h -> "content-type".equals(h.getKey().toLowerCase())).findFirst().map(Param::getKey)
                        .orElse(config.getDataset().getBody().getType().getContentType());
                if (!contentType.contains("charset")) {
                    contentType = contentType + "; charset=utf8";
                }

                config.getDataset().getHeaders().stream().filter(h -> "content-type".equals(h.getKey().toLowerCase())).findFirst()
                        .orElse(new Param("Content-Type", "")).setValue(contentType);
            }
        }

        // Manage authentication
        switch (config.getDataset().getDatastore().getAuthentication().getType()) {
        case Basic:
            BasicAuthConfigurer basic = new BasicAuthConfigurer();
            basic.configure(connection, configuration);
            break;
        case Digest:
            DigestAuthConfigurer digest = new DigestAuthConfigurer();
            digest.configure(connection, configuration);
            break;
        case Bearer:
            BearerAuthConfigurer bearer = new BearerAuthConfigurer();
            bearer.configure(connection, configuration);
            break;
        }
    }

}
