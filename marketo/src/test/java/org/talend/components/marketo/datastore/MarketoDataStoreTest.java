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