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
import org.talend.components.rest.configuration.RequestConfig;

@Slf4j
public class DigestAuthConfigurer extends org.talend.component.common.service.http.DigestAuthConfigurer {

    @Override
    public void configure(final Connection connection, final ConfigurerConfiguration configuration) {
        final RequestConfig config = configuration.get("configuration", RequestConfig.class);
        final Client client = configuration.get("httpClient", Client.class);

        if (config.getDataset().getConnectionTimeout() != null) {
            connection.withConnectionTimeout(config.getDataset().getConnectionTimeout());
        }
        if (config.getDataset().getReadTimeout() != null) {
            connection.withReadTimeout(config.getDataset().getReadTimeout());
        }

        super.configure(connection, configuration);
    }

}
