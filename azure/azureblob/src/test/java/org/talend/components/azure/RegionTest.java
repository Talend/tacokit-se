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
package org.talend.components.azure;

import java.net.URISyntaxException;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;
import org.talend.components.azure.common.service.AzureComponentServices;

import com.microsoft.azure.storage.CloudStorageAccount;

public class RegionTest {

    @Test
    public void testAccountAuthDefaultRegion() throws URISyntaxException {
        AzureStorageConnectionAccount accountConnection = new AzureStorageConnectionAccount();
        accountConnection.setAccountName("myaccount");
        accountConnection.setAccountKey("myaccountkey");

        AzureComponentServices service = new AzureComponentServices();
        CloudStorageAccount csa = service.createStorageAccount(accountConnection);

        Assert.assertEquals("myaccount", csa.getCredentials().getAccountName().toString());
        Assert.assertNull(csa.getEndpointSuffix());
        Assert.assertEquals("https://myaccount.blob.core.windows.net", csa.getBlobEndpoint().toString());
    }

    @Test
    public void testAccountAuthNotDefaultRegion() throws URISyntaxException {
        AzureStorageConnectionAccount accountConnection = new AzureStorageConnectionAccount();
        accountConnection.setAccountName("myaccount");
        accountConnection.setAccountKey("myaccountkey");

        AzureComponentServices service = new AzureComponentServices();
        CloudStorageAccount csa = service.createStorageAccount(accountConnection, "core.chinacloudapi.cn");

        Assert.assertEquals("myaccount", csa.getCredentials().getAccountName().toString());
        Assert.assertEquals("core.chinacloudapi.cn", csa.getEndpointSuffix());
        Assert.assertEquals("https://myaccount.blob.core.chinacloudapi.cn", csa.getBlobEndpoint().toString());
    }

    @Test
    public void testSignatureAuthDefaultRegion() throws URISyntaxException {
        AzureStorageConnectionSignature accountConnection = new AzureStorageConnectionSignature();
        accountConnection.setAzureSharedAccessSignature("https://myaccount.blob.core.windows.net/mytoken");

        AzureComponentServices service = new AzureComponentServices();
        CloudStorageAccount csa = service.createStorageAccount(accountConnection);

        Assert.assertEquals("core.windows.net", csa.getEndpointSuffix());
        Assert.assertEquals("https://myaccount.blob.core.windows.net", csa.getBlobEndpoint().toString());
    }

    @Test
    public void testSignatureAuthNotDefaultRegion() throws URISyntaxException {
        AzureStorageConnectionSignature accountConnection = new AzureStorageConnectionSignature();
        accountConnection.setAzureSharedAccessSignature("https://myaccount.blob.core.chinacloudapi.cn/mytoken");

        AzureComponentServices service = new AzureComponentServices();
        CloudStorageAccount csa = service.createStorageAccount(accountConnection);

        Assert.assertEquals("core.chinacloudapi.cn", csa.getEndpointSuffix());
        Assert.assertEquals("https://myaccount.blob.core.chinacloudapi.cn", csa.getBlobEndpoint().toString());
    }

    @Test
    public void testRegionUtils() throws URISyntaxException {

    }

}
