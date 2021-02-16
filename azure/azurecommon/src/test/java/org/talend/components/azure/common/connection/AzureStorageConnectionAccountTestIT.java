package org.talend.components.azure.common.connection;

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

class AzureStorageConnectionAccountTestIT {

    @Test
    void testSerial() throws IOException, ClassNotFoundException {
        final AzureStorageConnectionAccount account = new AzureStorageConnectionAccount();
        account.setAccountKey("mykey");
        account.setAccountName("MyAccountName");
        account.setAuthType(AzureAuthType.ACTIVE_DIRECTORY_CLIENT_CREDENTIAL);
        account.setProtocol(Protocol.HTTPS);
        account.setActiveDirProperties(new AzureConnectionActiveDir());
        account.getActiveDirProperties().setClientId("MyClientId");
        account.getActiveDirProperties().setClientSecret("MySecret");
        account.getActiveDirProperties().setTenantId("MyTenantId");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(account);

        ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final AzureStorageConnectionAccount cnxCopy = (AzureStorageConnectionAccount) ois.readObject();
        Assertions.assertEquals(account, cnxCopy);

    }
}