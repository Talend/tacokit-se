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
package org.talend.components.dynamicscrm.service;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.naming.AuthenticationException;

import org.apache.olingo.client.api.communication.request.retrieve.EdmMetadataRequest;
import org.apache.olingo.client.api.communication.request.retrieve.ODataEntitySetRequest;
import org.apache.olingo.client.api.communication.response.ODataRetrieveResponse;
import org.apache.olingo.client.api.domain.ClientEntitySet;
import org.apache.olingo.client.api.uri.URIBuilder;
import org.apache.olingo.commons.api.edm.Edm;
import org.talend.components.dynamicscrm.datastore.AppType;
import org.talend.components.dynamicscrm.datastore.DynamicsCrmConnection;
import org.talend.components.dynamicscrm.source.DynamicsCrmQueryResultsIterator;
import org.talend.ms.crm.odata.ClientConfiguration;
import org.talend.ms.crm.odata.ClientConfigurationFactory;
import org.talend.ms.crm.odata.DynamicsCRMClient;
import org.talend.ms.crm.odata.QueryOptionConfig;
import org.talend.sdk.component.api.service.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class DynamicsCrmService {

    public static final int INTERVAL_TIME = 1000;

    @Service
    private I18n i18n;

    public DynamicsCRMClient createClient(DynamicsCrmConnection connection, String entitySet) throws AuthenticationException {
        ClientConfiguration clientConfig;
        if (connection.getAppType() == AppType.NATIVE) {
            clientConfig = ClientConfigurationFactory.buildOAuthNativeClientConfiguration(connection.getClientId(),
                    connection.getUsername(), connection.getPassword(), connection.getAuthorizationEndpoint());
        } else {
            clientConfig = ClientConfigurationFactory.buildOAuthWebClientConfiguration(connection.getClientId(),
                    connection.getClientSecret(), connection.getUsername(), connection.getPassword(),
                    connection.getAuthorizationEndpoint(), ClientConfiguration.WebAppPermission.DELEGATED);
        }
        clientConfig.setTimeout(connection.getTimeout());
        clientConfig.setMaxRetry(connection.getMaxRetries(), INTERVAL_TIME);
        clientConfig.setReuseHttpClient(false);
        return new DynamicsCRMClient(clientConfig, connection.getServiceRootUrl(), entitySet);
    }

    public List<String> getEntitySetNames(DynamicsCrmConnection connection) {
        try {
            DynamicsCRMClient client = createClient(connection, null);
            ODataEntitySetRequest<ClientEntitySet> request = client.createEndpointsNamesRequest();
            ODataRetrieveResponse<ClientEntitySet> response = request.execute();
            ClientEntitySet entitySet = response.getBody();
            return entitySet.getEntities().stream().map(e -> e.getProperty("name").getValue().asPrimitive().toString())
                    .collect(Collectors.toList());
        } catch (AuthenticationException e) {
            throw new DynamicsCrmException(i18n.authenticationFailed(e.getMessage()), e);
        } catch (Exception e) {
            throw new DynamicsCrmException(i18n.entitySetRetrieveFailed(e.getMessage()), e);
        }
    }

    public DynamicsCrmQueryResultsIterator getEntitySetIterator(DynamicsCRMClient client, QueryOptionConfig config) {
        ODataEntitySetRequest<ClientEntitySet> request = client.createEntityRetrieveRequest(config);
        ODataRetrieveResponse<ClientEntitySet> response = request.execute();
        return new DynamicsCrmQueryResultsIterator(client, config, response.getBody());
    }

    public Edm getMetadata(DynamicsCRMClient client) {
        EdmMetadataRequest metadataRequest = client.createMetadataRetrieveRequest();
        Edm metadata;
        try {
            ODataRetrieveResponse<Edm> metadataResponse = metadataRequest.execute();
            metadata = metadataResponse.getBody();
        } catch (Exception e) {
            throw new DynamicsCrmException(i18n.metadataRetrieveFailed(e.getMessage()), e);
        }
        return metadata;
    }

    protected URIBuilder createUriBuilderForValidProps(DynamicsCRMClient client, DynamicsCrmConnection datastore,
            String entitySetName) {
        return client.getClient().newURIBuilder(datastore.getServiceRootUrl()).appendEntitySetSegment("EntityDefinitions")
                .appendKeySegment(Collections.singletonMap("LogicalName", entitySetName)).appendEntitySetSegment("Attributes")
                .select("LogicalName", "IsValidForRead", "IsValidForUpdate", "IsValidForCreate");
    }

    public List<PropertyValidationData> getPropertiesValidationData(DynamicsCRMClient client, DynamicsCrmConnection datastore,
            String logicalTypeName) {
        ODataEntitySetRequest<ClientEntitySet> validationDataRequest = client
                .createRequest(createUriBuilderForValidProps(client, datastore, logicalTypeName));
        ODataRetrieveResponse<ClientEntitySet> validationDataResponse = validationDataRequest.execute();
        ClientEntitySet validationDataSet = validationDataResponse.getBody();
        return validationDataSet.getEntities().stream()
                .map(e -> new PropertyValidationData((String) e.getProperty("LogicalName").getPrimitiveValue().toValue(),
                        (boolean) e.getProperty("IsValidForCreate").getPrimitiveValue().toValue(),
                        (boolean) e.getProperty("IsValidForUpdate").getPrimitiveValue().toValue(),
                        (boolean) e.getProperty("IsValidForRead").getPrimitiveValue().toValue()))
                .collect(Collectors.toList());
    }

}