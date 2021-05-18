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
package org.talend.components.azure.dataset;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.AzureAuthType;
import org.talend.components.AzureConnectionActiveDir;
import org.talend.components.azure.common.Protocol;
import org.talend.components.azure.common.connection.AzureStorageConnectionAccount;
import org.talend.components.azure.common.connection.AzureStorageConnectionSignature;
import org.talend.components.azure.common.csv.CSVFormatOptions;
import org.talend.components.azure.common.csv.RecordDelimiter;
import org.talend.components.azure.datastore.AzureCloudConnection;

class AzureBlobDatasetTestIT {

    @Test
    void testSerial() throws IOException, ClassNotFoundException {
        final AzureBlobDataset dataset = new AzureBlobDataset();
        dataset.setConnection(new AzureCloudConnection());
        dataset.getConnection().setAccountConnection(new AzureStorageConnectionAccount());
        dataset.getConnection().getAccountConnection().setAccountName("TheAccountName");
        dataset.getConnection().getAccountConnection().setAccountKey("TheKey");
        dataset.getConnection().getAccountConnection().setAuthType(AzureAuthType.BASIC);

        dataset.getConnection().setSignatureConnection(new AzureStorageConnectionSignature());
        dataset.getConnection().getSignatureConnection().setAzureSharedAccessSignature("{ signature }");
        dataset.getConnection().setEndpointSuffix("myendpoint");
        dataset.getConnection().setUseAzureSharedSignature(true);

        dataset.setCsvOptions(new CSVFormatOptions());
        dataset.getCsvOptions().setRecordDelimiter(RecordDelimiter.LF);

        dataset.setContainerName("MyContainer");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(dataset);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final AzureBlobDataset dsCopy = (AzureBlobDataset) ois.readObject();
        Assertions.assertEquals(dataset, dsCopy);

    }
}