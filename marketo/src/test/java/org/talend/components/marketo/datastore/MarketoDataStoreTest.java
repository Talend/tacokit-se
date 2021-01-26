package org.talend.components.marketo.datastore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class MarketoDataStoreTest {

    @Test
    void serial() throws IOException, ClassNotFoundException {
        final MarketoDataStore connection = new MarketoDataStore();

        connection.setClientId("clientId");
        connection.setClientSecret("clientSecret");
        connection.setEndpoint("http://endpoint.com/");

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(out);
        oos.writeObject(connection);

        final ByteArrayInputStream input = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream ois = new ObjectInputStream(input);
        final MarketoDataStore cnxCopy = (MarketoDataStore) ois.readObject();
        Assertions.assertEquals(connection, cnxCopy);

        Assertions.assertEquals(connection.toString(), cnxCopy.toString());
    }
}