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

import java.util.Base64;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.talend.components.azure.common.AzureConnection;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableRequestOptions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AzureConnectionServiceTest {

    @Test
    public void testExecuteQuery() throws Exception {
        String someTableName = "someTableName";
        CloudStorageAccount mockedAccount = Mockito.mock(CloudStorageAccount.class);
        TableQuery mockedQuery = Mockito.mock(TableQuery.class);
        CloudTableClient mockedTableClient = Mockito.mock(CloudTableClient.class);
        CloudTable mockedTable = Mockito.mock(CloudTable.class);
        Mockito.when(mockedTableClient.getTableReference(someTableName)).thenReturn(mockedTable);
        TableRequestOptions mockedTableRequestOptions = Mockito.mock(TableRequestOptions.class);
        Mockito.when(mockedTableClient.getDefaultRequestOptions()).thenReturn(mockedTableRequestOptions);
        Mockito.when(mockedAccount.createCloudTableClient()).thenReturn(mockedTableClient);
        new AzureConnectionService().executeQuery(mockedAccount, someTableName, mockedQuery);

        Mockito.verify(mockedTable).execute(mockedQuery, null, AzureConnectionService.getTalendOperationContext());
    }

    @Test
    public void testCreateStorageAccountWithPass() throws Exception {
        AzureConnection connectionProperties = new AzureConnection();
        String expectedAccountKey = "someAccountName";
        connectionProperties.setUseAzureSharedSignature(false);
        connectionProperties.setAccountName(expectedAccountKey);
        String accountKey = "someKey";
        connectionProperties.setAccountKey(new String(Base64.getEncoder().encode(accountKey.getBytes())));
        CloudStorageAccount account = new AzureConnectionService().createStorageAccount(connectionProperties);

        assertNotNull(account);
        assertEquals(expectedAccountKey, account.getCredentials().getAccountName());
    }

}