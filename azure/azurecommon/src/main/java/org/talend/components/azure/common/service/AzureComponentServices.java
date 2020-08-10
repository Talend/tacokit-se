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
package org.talend.components.azure.common.service;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.AzureConnectionActiveDir;
import org.talend.components.azure.common.Protocol;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;
import org.talend.components.azure.runtime.token.AzureActiveDirectoryTokenGetter;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageCredentialsToken;
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

    public static final String SAS_PATTERN = "(http.?)?://(.*)\\.(blob|file|queue|table)\\.(.*)/(.*)";

    @Service
    private MessageService i18nService;

    public CloudStorageAccount createStorageAccount(AzureStorageConnectionAccount azureConnection) throws URISyntaxException {
        return createStorageAccount(azureConnection, null);
    }

    public CloudStorageAccount createStorageAccount(AzureStorageConnectionAccount azureConnection, String endpointSuffix)
            throws URISyntaxException {
        if (azureConnection == null || StringUtils.isEmpty(azureConnection.getAccountName())) {
            throw new IllegalArgumentException(i18nService.connectionIsNull());
        }

        switch (azureConnection.getAuthType()) {
        case BASIC:
            return createStorageAccountBasic(azureConnection, endpointSuffix);
        case ACTIVE_DIRECTORY_CLIENT_CREDENTIAL:
            return createStorageAccountAD(azureConnection, endpointSuffix);
        default:
            throw new IllegalArgumentException("Not supported auth type selected"); // shouldn't go here
        }
    }

    public CloudStorageAccount createStorageAccount(AzureStorageConnectionSignature azureConnection) throws URISyntaxException {
        if (azureConnection == null || StringUtils.isEmpty(azureConnection.getAzureSharedAccessSignature())) {
            throw new IllegalArgumentException(i18nService.connectionIsNull());
        }
        Matcher matcher = Pattern.compile(SAS_PATTERN).matcher(azureConnection.getAzureSharedAccessSignature());
        if (!matcher.matches()) {
            throw new IllegalArgumentException(i18nService.wrongSASFormat());
        }

        StorageCredentials credentials = new StorageCredentialsSharedAccessSignature(matcher.group(5));

        return new CloudStorageAccount(credentials, "https".equals(matcher.group(1)), matcher.group(4), matcher.group(2));
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

    private CloudStorageAccount createStorageAccountBasic(AzureStorageConnectionAccount connectionProperties,
            String endpointSuffix) throws URISyntaxException {
        if (StringUtils.isEmpty(connectionProperties.getAccountKey())) {
            throw new IllegalArgumentException(i18nService.connectionIsNull());
        }

        try {
            StorageCredentials credentials = new StorageCredentialsAccountAndKey(connectionProperties.getAccountName(),
                    connectionProperties.getAccountKey());
            return new CloudStorageAccount(credentials, connectionProperties.getProtocol() == Protocol.HTTPS, endpointSuffix,
                    null);
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(i18nService.invalidAccountKeyFormat(e.getMessage()), e);
        }
    }

    private CloudStorageAccount createStorageAccountAD(AzureStorageConnectionAccount azureConnection, String endpointSuffix) {
        AzureConnectionActiveDir activeDirProperties = azureConnection.getActiveDirProperties();
        if (activeDirProperties == null || StringUtils.isEmpty(activeDirProperties.getClientId())
                || StringUtils.isEmpty(activeDirProperties.getTenantId())
                || StringUtils.isEmpty(activeDirProperties.getClientSecret())) {
            throw new IllegalArgumentException(i18nService.connectionIsNull());
        }
        AzureActiveDirectoryTokenGetter tokenGetter = new AzureActiveDirectoryTokenGetter(activeDirProperties.getTenantId(),
                activeDirProperties.getClientId(), activeDirProperties.getClientSecret());
        try {
            String token = tokenGetter.retrieveAccessToken();

            StorageCredentials credentials = new StorageCredentialsToken(azureConnection.getAccountName(), token);
            return new CloudStorageAccount(credentials, true, endpointSuffix);
        } catch (Exception e) {
            throw new RuntimeException(i18nService.connectionError(e.getMessage()), e);
        }
    }

    public HealthCheckStatus testConnection(CloudStorageAccount cloudStorageAccount) {
        if (cloudStorageAccount == null) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nService.connectionIsNull());
        }

        final int maxContainers = 1;
        try {
            CloudBlobClient blobClient = createCloudBlobClient(cloudStorageAccount, DEFAULT_RETRY_POLICY);
            // will throw an exception if not authorized or account not exist
            blobClient.listContainersSegmented(null, null, maxContainers, null, null, getTalendOperationContext());
        } catch (Exception e) {
            String errorMessage = (StringUtils.isNotEmpty(e.getMessage()) || (e.getCause() == null)) ? e.getMessage()
                    : e.getCause().toString();
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18nService.connectionError(errorMessage));
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18nService.connected());
    }
}
