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
package org.talend.components.azure.service;

import static org.talend.components.azure.common.service.AzureComponentServices.SAS_PATTERN;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;

public class RegionUtils {

    private final static String AZURE_ACCOUNT_CRED_KEY_FORMAT = "fs.azure.account.key.%s.blob.%s";

    private final static String AZURE_SAS_CRED_KEY_FORMAT = "fs.azure.sas.%s.%s.blob.%s";

    private final static String AZURE_URI_FORMAT = "wasb%s://%s@%s.blob.%s/%s";

    private final static Pattern sasPattern = Pattern.compile(SAS_PATTERN);

    private Matcher matcher;

    public RegionUtils(AzureStorageConnectionSignature ascs) {
        matcher = sasPattern.matcher(ascs.getAzureSharedAccessSignature());
        if (!matcher.matches()) {
            throw new RuntimeException("SAS URL format is not right, please check it");
        }
    }

    public String getAccountName4SignatureAuth() {
        return matcher.group(2);
    }

    public String getEndpointSuffix4SignatureAuth() {
        return matcher.group(4);
    }

    public String getToken4SignatureAuth() {
        return matcher.group(5);
    }

    public static String getSasKey4SignatureAuth(String containerName, String accountName, String endpointSuffix) {
        String sasKey = String.format(AZURE_SAS_CRED_KEY_FORMAT, containerName, accountName, endpointSuffix);
        return sasKey;
    }

    public static String getAccountCredKey4AccountAuth(String accountName, String endpointSuffix) {
        String accountCredKey = String.format(AZURE_ACCOUNT_CRED_KEY_FORMAT, accountName, endpointSuffix);
        return accountCredKey;
    }

    public static String getBlobURI(boolean isHttpsConnectionUsed, String containerName, String accountName,
            String endpointSuffix, String itemName) {
        String blobURI = String.format(AZURE_URI_FORMAT, isHttpsConnectionUsed ? "s" : "", containerName, accountName,
                endpointSuffix, itemName);
        return blobURI;

    }
}
