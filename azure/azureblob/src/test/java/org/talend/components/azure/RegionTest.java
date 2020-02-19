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

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;
import org.talend.components.azure.common.service.AzureComponentServices;
import org.talend.components.azure.service.RegionUtils;

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
        String[] regions = { "core.chinacloudapi.cn", "core.windows.net" };
        for (String region : regions) {
            AzureStorageConnectionSignature accountConnection = new AzureStorageConnectionSignature();
            accountConnection.setAzureSharedAccessSignature("https://myaccount.blob." + region + "/mytoken");
            RegionUtils ru = new RegionUtils(accountConnection);
            Assert.assertEquals("myaccount", ru.getAccountName4SignatureAuth());
            Assert.assertEquals(region, ru.getEndpointSuffix4SignatureAuth());
            Assert.assertEquals("mytoken", ru.getToken4SignatureAuth());
            Assert.assertEquals("fs.azure.sas.mycontainer.myaccount.blob." + region, RegionUtils.getSasKey4SignatureAuth(
                    "mycontainer", ru.getAccountName4SignatureAuth(), ru.getEndpointSuffix4SignatureAuth()));
            Assert.assertEquals("fs.azure.account.key.myaccount.blob." + region,
                    RegionUtils.getAccountCredKey4AccountAuth("myaccount", region));
            Assert.assertEquals("wasbs://mycontainer@myaccount.blob." + region + "/myitem",
                    RegionUtils.getBlobURI(true, "mycontainer", "myaccount", region, "myitem"));
        }
    }

}
