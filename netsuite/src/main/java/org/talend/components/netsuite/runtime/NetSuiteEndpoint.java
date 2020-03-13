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
package org.talend.components.netsuite.runtime;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.datastore.NetSuiteDataStore;
import org.talend.components.netsuite.datastore.NetSuiteDataStore.LoginType;
import org.talend.components.netsuite.runtime.client.NetSuiteClientFactory;
import org.talend.components.netsuite.runtime.client.NetSuiteClientService;
import org.talend.components.netsuite.runtime.client.NetSuiteCredentials;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.client.NetSuiteVersion;
import org.talend.components.netsuite.runtime.client.NsTokenPassport;
import org.talend.components.netsuite.service.Messages;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Represents NetSuite Web Service endpoint.
 */
public class NetSuiteEndpoint {

    public static final String CONNECTION = "NetSuite_Connection";

    /** Creates instance of NetSuite client. */
    private NetSuiteClientFactory<?> clientFactory;

    /** Connection configuration for this endpoint. */
    private ConnectionConfig connectionConfig;

    private Messages i18n;

    /**
     * Creates new instance using given client factory and connection configuration.
     *
     * @param clientFactory client factory
     * @param dataStore data store
     */
    public NetSuiteEndpoint(NetSuiteClientFactory<?> clientFactory, Messages i18n, NetSuiteDataStore dataStore) {
        this.clientFactory = clientFactory;
        this.i18n = i18n;
        this.connectionConfig = createConnectionConfig(dataStore);
    }

    /**
     * Create connection configuration for given connection properties.
     *
     * @param properties connection properties
     * @return connection configuration
     * @throws NetSuiteException if connection configuration not valid
     */
    public ConnectionConfig createConnectionConfig(NetSuiteDataStore properties) throws NetSuiteException {
        validateProperties(properties);

        NetSuiteCredentials credentials = null;
        NsTokenPassport tokenPassport = null;
        if (properties.getLoginType() == LoginType.BASIC) {
            credentials = new NetSuiteCredentials(properties.getEmail(), properties.getPassword(), properties.getAccount(),
                    properties.getRole().trim(), properties.getApplicationId());
        } else {
            tokenPassport = new NsTokenPassport(properties.getAccount(), properties.getConsumerKey(),
                    properties.getConsumerSecret(), properties.getTokenId(), properties.getTokenSecret());
        }
        NetSuiteVersion apiVersion = NetSuiteVersion.parseVersion(properties.getApiVersion());
        return new ConnectionConfig(properties.getApiVersion().getEndpoint(), apiVersion.getMajor(), credentials, tokenPassport);
    }

    private void validateProperties(NetSuiteDataStore properties) {
        if (StringUtils.isEmpty(properties.getApiVersion().getEndpoint())) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.endpointUrlRequired());
        }
        if (StringUtils.isEmpty(properties.getAccount())) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.accountRequired());
        }

        if (properties.getLoginType() == LoginType.BASIC) {
            if (StringUtils.isEmpty(properties.getEmail())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.emailRequired());
            }
            if (StringUtils.isEmpty(properties.getPassword())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.passwordRequired());
            }

            if (properties.getRole() == null || properties.getRole().trim().isEmpty()) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.roleRequired());
            }
        } else {
            if (StringUtils.isEmpty(properties.getConsumerKey())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.consumerKeyRequired());
            }
            if (StringUtils.isEmpty(properties.getConsumerSecret())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.consumerSecretRequired());
            }
            if (StringUtils.isEmpty(properties.getTokenId())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.tokenIdRequired());
            }
            if (StringUtils.isEmpty(properties.getTokenSecret())) {
                throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.tokenSecretRequired());
            }
        }
    }

    /**
     * Return NetSuite client.
     *
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    public NetSuiteClientService<?> getClientService() throws NetSuiteException {
        return connect(connectionConfig);
    }

    /**
     * Creates new NetSuite client and connects to NetSuite remote endpoint.
     *
     * @param connectionConfig connection configuration
     * @return client
     * @throws NetSuiteException if an error occurs during connecting
     */
    private NetSuiteClientService<?> connect(ConnectionConfig connectionConfig) throws NetSuiteException {
        NetSuiteClientService<?> clientService = clientFactory.createClient();
        clientService.setI18n(i18n);
        clientService.setEndpointUrl(connectionConfig.getEndpointUrl());
        clientService.setCredentials(connectionConfig.getCredentials());
        clientService.setTokenPassport(connectionConfig.getTokenPassport());
        clientService.login();
        return clientService;
    }

    /**
     * Holds configuration for connecting to NetSuite.
     */
    @Data
    @EqualsAndHashCode
    @ToString
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ConnectionConfig {

        private String endpointUrl;

        private NetSuiteVersion apiVersion;

        private NetSuiteCredentials credentials;

        private NsTokenPassport tokenPassport;
    }
}