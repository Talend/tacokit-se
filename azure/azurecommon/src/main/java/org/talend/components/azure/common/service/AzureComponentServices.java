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

package org.talend.components.azure.common.service;

import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.azure.common.Protocol;
import org.talend.components.azure.common.connection.AzureStorageConnectable;
import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.blob.CloudBlobClient;

@Service
public class AzureComponentServices {

    public static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryExponentialRetry(10, 3);

    private static final String USER_AGENT_KEY = "User-Agent";

    private static final String USER_AGENT_FORMAT = "APN/1.0 Talend/%s TaCoKit/%s";

    /**
     * Would be set as User-agent when real user-agent creation would fail
     */
    private static final String UNKNOWN_VERSION = "UNKNOWN";

    private static String applicationVersion = UNKNOWN_VERSION;

    private static String componentVersion = UNKNOWN_VERSION;

    private static OperationContext talendOperationContext;

    @Service
    private MessageService i18nService;

    public CloudStorageAccount createStorageAccount(AzureStorageConnectionAccount azureConnection) throws URISyntaxException {
        StorageCredentials credentials = new StorageCredentialsAccountAndKey(azureConnection.getAccountName(),
                azureConnection.getAccountKey());

        return new CloudStorageAccount(credentials, azureConnection.getProtocol() == Protocol.HTTPS);
    }

    public CloudStorageAccount createStorageAccount(AzureStorageConnectionSignature azureConnection) throws URISyntaxException {
        StorageCredentials credentials = new StorageCredentialsSharedAccessSignature(
                azureConnection.getAzureSharedAccessSignature());

        return new CloudStorageAccount(credentials);
    }

    public CloudBlobClient createCloudBlobClient(CloudStorageAccount connection, RetryPolicy retryPolicy) {
        CloudBlobClient blobClient = connection.createCloudBlobClient();
        blobClient.getDefaultRequestOptions().setRetryPolicyFactory(retryPolicy);

        return blobClient;
    }

    public static OperationContext getTalendOperationContext() {
        if (talendOperationContext == null) {
            talendOperationContext = new OperationContext();
            HashMap<String, String> talendUserHeaders = new HashMap<>();
            talendUserHeaders.put(USER_AGENT_KEY, getUserAgentString());
            talendOperationContext.setUserHeaders(talendUserHeaders);
        }

        return talendOperationContext;
    }

    public static void setApplicationVersion(String applicationVersion) {
        if (StringUtils.isNotEmpty(applicationVersion)) {
            AzureComponentServices.applicationVersion = applicationVersion;
        }
    }

    public static void setComponentVersion(String componentVersion) {
        if (StringUtils.isNotEmpty(componentVersion)) {
            AzureComponentServices.componentVersion = componentVersion;
        }
    }

    private static String getUserAgentString() {
        return String.format(USER_AGENT_FORMAT, applicationVersion, componentVersion);
    }

    public HealthCheckStatus testConnection(AzureStorageConnectable azureConnection) {
        final int maxContainers = 1;
        try {
            CloudStorageAccount cloudStorageAccount = azureConnection instanceof AzureStorageConnectionSignature
                    ? createStorageAccount((AzureStorageConnectionSignature) azureConnection)
                    : createStorageAccount((AzureStorageConnectionAccount) azureConnection);
            CloudBlobClient blobClient = createCloudBlobClient(cloudStorageAccount, DEFAULT_RETRY_POLICY);
            // will throw an exception if not authorized or account not exist
            blobClient.listContainersSegmented(null, null, maxContainers, null, null, getTalendOperationContext());
        } catch (Exception e) {
            String errorMessage = (StringUtils.isNotEmpty(e.getMessage()) || (e.getCause() == null)) ? e.getMessage()
                    : e.getCause().toString();
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nService.connectionError() + ": " + errorMessage);
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18nService.connected());
    }
}
