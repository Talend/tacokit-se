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
package org.talend.components.azure;

import java.net.URISyntaxException;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.azure.datastore.AzureCloudConnection;
import org.talend.components.azure.service.AzureBlobComponentServices;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

@WithComponents("org.talend.components.azure")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class BaseIT {

    @Injected
    protected BaseComponentsHandler componentsHandler;

    protected static String containerName;

    protected static CloudStorageAccount storageAccount;

    protected static AzureCloudConnection dataStore;

    @BeforeAll
    public void initContainer() throws Exception {
        containerName = "test-it-" + RandomStringUtils.randomAlphabetic(10).toLowerCase();
        dataStore = BlobTestUtils.createCloudConnection();
        AzureBlobComponentServices componentService = componentsHandler.findService(AzureBlobComponentServices.class);

        storageAccount = componentService.createStorageAccount(dataStore);
        BlobTestUtils.createStorage(containerName, storageAccount);
    }

    @AfterEach
    public void cleanUpContainer() throws URISyntaxException, StorageException {
        CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
        CloudBlobContainer container = blobClient.getContainerReference(containerName);

        for (ListBlobItem blob : container.listBlobs("", true)) {
            if (blob instanceof CloudBlob) {
                ((CloudBlob) blob).delete();
            }
        }
    }

    @AfterAll
    public static void removeContainer() throws URISyntaxException, StorageException {
        BlobTestUtils.deleteStorage(containerName, storageAccount);
    }
}
