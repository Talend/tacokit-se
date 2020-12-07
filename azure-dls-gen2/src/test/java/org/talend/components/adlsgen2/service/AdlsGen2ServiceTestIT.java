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
package org.talend.components.adlsgen2.service;

import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.components.adlsgen2.datastore.AdlsGen2Connection.AuthMethod;
import org.talend.components.adlsgen2.datastore.Constants.HeaderConstants;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2ServiceTestIT extends AdlsGen2TestBase {

    @Service
    AdlsGen2Service service;

    @Service
    AdlsGen2APIClient serviceTestClient;

    @Test
    void getClient() {
        serviceTestClient = service.getClient(connection);
        assertNotNull(serviceTestClient);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void filesystemList(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        List<String> result = service.filesystemList(datastoreRuntimeInfo);
        assertNotNull(result);
        assertTrue(result.size() >= 0);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void pathList(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        inputConfiguration.getDataSet().setBlobPath("");
        Object result = service.pathList(datasetRuntimeInfo);
        assertNotNull(result);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void pathListInexistent(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        String path = "myNewFolderZZZ";
        inputConfiguration.getDataSet().setBlobPath(path);
        Object result = null;
        try {
            result = service.pathList(datasetRuntimeInfo);
            fail("The path should not exist");
        } catch (Exception e) {
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void extractFolderPath(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        String blobPath = "folder01/folder02/newFolder01/blob.txt";
        assertEquals("folder01/folder02/newFolder01", service.extractFolderPath(blobPath));
        blobPath = "/folder01/folder02/newFolder01/blob.txt";
        assertEquals("/folder01/folder02/newFolder01", service.extractFolderPath(blobPath));
        blobPath = "newFolder01/blob.txt";
        assertEquals("newFolder01", service.extractFolderPath(blobPath));
        blobPath = "/newFolder01/blob.txt";
        assertEquals("/newFolder01", service.extractFolderPath(blobPath));
        blobPath = "/blob.txt";
        assertEquals("/", service.extractFolderPath(blobPath));
        blobPath = "blob.txt";
        assertEquals("/", service.extractFolderPath(blobPath));
        blobPath = "";
        assertEquals("/", service.extractFolderPath(blobPath));
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void pathExists(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        // paths should exists
        String blobPath = "demo_gen2/in/parquet_file.parquet";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertTrue(service.pathExists(datasetRuntimeInfo));
        // paths do not exist
        blobPath = "myNewFolder/subfolder03/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertFalse(service.pathExists(datasetRuntimeInfo));
        blobPath = "newFolder01ZZZ/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertFalse(service.pathExists(datasetRuntimeInfo));
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void pathReadParquetFile(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        String path = "demo_gen2/in/parquet_file.parquet";
        inputConfiguration.getDataSet().setBlobPath(path);
        inputConfiguration.getDataSet().setFormat(FileFormat.PARQUET);
        Iterator<Record> result = service.pathRead(datasetRuntimeInfo);
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SharedKey", "SAS" })
    void pathReadAvroFile(String authmethod) {
        connection.setAuthMethod(AuthMethod.valueOf(authmethod));
        String path = "demo_gen2/in/userdata1.avro";
        inputConfiguration.getDataSet().setBlobPath(path);
        inputConfiguration.getDataSet().setFormat(FileFormat.AVRO);
        Iterator<Record> result = service.pathRead(datasetRuntimeInfo);
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
        }
        // another one
        path = "demo_gen2/in/customers.avro";
        inputConfiguration.getDataSet().setBlobPath(path);
        result = service.pathRead(datasetRuntimeInfo);
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
        }
    }

    @Test
    void testUserAgentValue() {
        assertEquals("APN/1.0 Talend/2019 tck/1.1.9", HeaderConstants.USER_AGENT_AZURE_DLS_GEN2);
    }
}
