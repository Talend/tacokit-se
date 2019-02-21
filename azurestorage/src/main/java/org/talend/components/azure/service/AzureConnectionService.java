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
package org.talend.components.azure.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.azure.common.AzureConnection;
import org.talend.components.azure.common.Protocol;
import org.talend.sdk.component.api.service.Service;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageErrorCodeStrings;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableServiceException;

@Service
public class AzureConnectionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureConnectionService.class);

    public static final RetryPolicy DEFAULT_RETRY_POLICY = new RetryExponentialRetry(10, 3);

    static final String USER_AGENT_KEY = "User-Agent";

    /**
     * Would be set as User-agent when real user-agent creation would fail
     */
    private static final String USER_AGENT_FORMAT = "APN/1.0 Talend/%s TaCoKit/%s";

    private static final String UNKNOWN_VERSION = "UNKNOWN";

    public static final int DEFAULT_CREATE_TABLE_TIMEOUT = 50000;

    private static String applicationVersion = UNKNOWN_VERSION;

    private static String componentVersion = UNKNOWN_VERSION;

    private static OperationContext talendOperationContext;

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
            AzureConnectionService.applicationVersion = applicationVersion;
        }
    }

    public static void setComponentVersion(String componentVersion) {
        if (StringUtils.isNotEmpty(componentVersion)) {
            AzureConnectionService.componentVersion = componentVersion;
        }
    }

    private static String getUserAgentString() {
        return String.format(USER_AGENT_FORMAT, applicationVersion, componentVersion);
    }

    public Iterable<DynamicTableEntity> executeQuery(CloudStorageAccount storageAccount, String tableName,
            TableQuery<DynamicTableEntity> partitionQuery) throws URISyntaxException, StorageException {
        LOGGER.debug("Executing query for table {} with filter: {}", tableName, partitionQuery.getFilterString());
        CloudTable cloudTable = createTableClient(storageAccount, tableName);
        return cloudTable.execute(partitionQuery, null, getTalendOperationContext());
    }

    public CloudStorageAccount createStorageAccount(AzureConnection azureConnection) throws URISyntaxException {
        StorageCredentials credentials = null;
        if (!azureConnection.isUseAzureSharedSignature()) {
            credentials = new StorageCredentialsAccountAndKey(azureConnection.getAccountName(), azureConnection.getAccountKey());
        } else {
            credentials = new StorageCredentialsSharedAccessSignature(azureConnection.getAzureSharedAccessSignature());
        }
        return new CloudStorageAccount(credentials, azureConnection.getProtocol() == Protocol.HTTPS);
    }

    public void createTable(CloudStorageAccount connection, String tableName) throws StorageException, URISyntaxException {
        CloudTable cloudTable = createTableClient(connection, tableName);
        cloudTable.create(null, getTalendOperationContext());
    }

    public void createTableIfNotExists(CloudStorageAccount connection, String tableName)
            throws StorageException, URISyntaxException {
        CloudTable cloudTable = createTableClient(connection, tableName);
        cloudTable.createIfNotExists(null, getTalendOperationContext());
    }

    public void deleteTableAndCreate(CloudStorageAccount connection, String tableName)
            throws URISyntaxException, StorageException, IOException {
        CloudTable cloudTable = createTableClient(connection, tableName);
        cloudTable.delete(null, getTalendOperationContext());
        createTableAfterDeletion(cloudTable);
    }

    /**
     * This method create a table after it's deletion.<br/>
     * the table deletion take about 40 seconds to be effective on azure CF.
     * https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Delete-Table#Remarks <br/>
     * So we try to wait 50 seconds if the first table creation return an
     * {@link StorageErrorCodeStrings.TABLE_BEING_DELETED } exception code
     *
     * @param cloudTable
     * @throws StorageException
     * @throws IOException
     */
    private void createTableAfterDeletion(CloudTable cloudTable) throws StorageException, IOException {
        try {
            cloudTable.create(null, getTalendOperationContext());
        } catch (TableServiceException e) {
            if (!e.getErrorCode().equals(StorageErrorCodeStrings.TABLE_BEING_DELETED)) {
                throw e;
            }
            LOGGER.warn("Table '{}' is currently being deleted. We'll retry in a few moments...", cloudTable.getName());
            // wait 50 seconds (min is 40s) before retrying.
            // See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Delete-Table#Remarks
            try {
                Thread.sleep(DEFAULT_CREATE_TABLE_TIMEOUT);
            } catch (InterruptedException eint) {
                throw new IOException("Wait process for recreating table interrupted.");
            }
            cloudTable.create(null, getTalendOperationContext());
            LOGGER.debug("Table {} created.", cloudTable.getName());
        }
    }

    public void deleteTableIfExistsAndCreate(CloudStorageAccount connection, String tableName)
            throws URISyntaxException, StorageException, IOException {
        CloudTable cloudTable = createTableClient(connection, tableName);
        cloudTable.deleteIfExists(null, getTalendOperationContext());
        createTableAfterDeletion(cloudTable);
    }

    CloudTableClient createCloudTableClient(CloudStorageAccount connection, RetryPolicy retryPolicy) {
        CloudTableClient tableClient = connection.createCloudTableClient();
        tableClient.getDefaultRequestOptions().setRetryPolicyFactory(retryPolicy);

        return tableClient;
    }

    public CloudTable createTableClient(CloudStorageAccount connection, String tableName)
            throws URISyntaxException, StorageException {
        return createTableClient(connection, tableName, DEFAULT_RETRY_POLICY);
    }

    public CloudTable createTableClient(CloudStorageAccount connection, String tableName, RetryPolicy retryPolicy)
            throws URISyntaxException, StorageException {
        return createCloudTableClient(connection, retryPolicy).getTableReference(tableName);
    }
}