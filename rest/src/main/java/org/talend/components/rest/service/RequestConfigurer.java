// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.rest.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.rest.configuration.RequestConfig;
import org.talend.sdk.component.api.service.http.configurer.oauth1.OAuth1;

@Slf4j
public class RequestConfigurer extends OAuth1.Configurer {

    @Override
    public void configure(final Connection connection, final ConfigurerConfiguration configuration) {
        final RequestConfig config = configuration.get("configuration", RequestConfig.class);

        if (config.getDataset().getConnectionTimeout() != null) {
            connection.withConnectionTimeout(config.getDataset().getConnectionTimeout());
        }
        if (config.getDataset().getReadTimeout() != null) {
            connection.withReadTimeout(config.getDataset().getReadTimeout());
        }

    }

}
