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
