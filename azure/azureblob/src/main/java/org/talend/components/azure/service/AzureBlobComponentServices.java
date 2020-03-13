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
package org.talend.components.azure.service;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.talend.components.azure.common.exception.BlobRuntimeException;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import lombok.Getter;

@Service
public class AzureBlobComponentServices {

    /**
     * The name of HealthCheck service
     */
    public static final String TEST_CONNECTION = "testConnection";

    public static final String GET_CONTAINER_NAMES = "getContainers";

    @Getter
    @Service
    AzureComponentServices connectionService;

    @Service
    private MessageService i18nService;

    @HealthCheck(TEST_CONNECTION)
    public HealthCheckStatus testConnection(@Option AzureCloudConnection azureConnection) {
        try {
            CloudStorageAccount cloudStorageAccount = azureConnection.isUseAzureSharedSignature()
                    ? connectionService.createStorageAccount(azureConnection.getSignatureConnection())
                    : connectionService.createStorageAccount(azureConnection.getAccountConnection(),
                            azureConnection.getEndpointSuffix());
            return connectionService.testConnection(cloudStorageAccount);
        } catch (URISyntaxException e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nService.illegalContainerName());
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
    }

    public CloudStorageAccount createStorageAccount(AzureCloudConnection azureConnection) throws URISyntaxException {
        return azureConnection.isUseAzureSharedSignature()
                ? connectionService.createStorageAccount(azureConnection.getSignatureConnection())
                : connectionService.createStorageAccount(azureConnection.getAccountConnection(),
                        azureConnection.getEndpointSuffix());
    }

    @Suggestions(GET_CONTAINER_NAMES)
    public SuggestionValues getContainerNames(@Option AzureCloudConnection azureConnection) {
        List<SuggestionValues.Item> containerNames = new ArrayList<>();
        try {
            CloudStorageAccount storageAccount = createStorageAccount(azureConnection);
            final OperationContext operationContext = AzureComponentServices.getTalendOperationContext();
            for (CloudBlobContainer container : connectionService
                    .createCloudBlobClient(storageAccount, AzureComponentServices.DEFAULT_RETRY_POLICY)
                    .listContainers(null, null, null, operationContext)) {
                containerNames.add(new SuggestionValues.Item(container.getName(), container.getName()));
            }

        } catch (Exception e) {
            throw new BlobRuntimeException(i18nService.errorRetrieveContainers(), e);
        }

        return new SuggestionValues(true, containerNames);
    }
}