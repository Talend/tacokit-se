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