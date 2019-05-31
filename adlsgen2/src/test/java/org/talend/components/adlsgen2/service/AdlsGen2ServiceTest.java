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
package org.talend.components.adlsgen2.service;

import java.util.Iterator;
import java.util.List;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.adlsgen2.common.format.FileFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.http.internal.impl.AzureStorageCredentialsRemovalResponseLocator;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

import lombok.extern.slf4j.Slf4j;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
@HttpApi(useSsl = true, responseLocator = AzureStorageCredentialsRemovalResponseLocator.class)
@WithComponents("org.talend.components.adlsgen2")
class AdlsGen2ServiceTest extends AdlsGen2TestBase {

    @Service
    AdlsGen2Service service;

    @Service
    AdlsGen2APIClient serviceTestClient;

    @Service
    AccessTokenProvider accessTokenProvider;

    @Test
    void getClient() {
        serviceTestClient = service.getClient(connection);
        assertNotNull(serviceTestClient);
    }

    @Test
    void accessToken() {
        String result = service.getAccessToken(connection);
        assertNotNull(result);
        assertTrue(result.length() > 100);
    }

    @Test
    void filesystemList() {
        log.warn("[filesystemList] {}", connection);
        List<String> result = service.filesystemList(connection);
        assertNotNull(result);
        assertTrue(result.size() >= 0);
    }

    @Test
    void pathList() {
        String path = "";
        inputConfiguration.getDataSet().setBlobPath(path);
        Object result = service.pathList(inputConfiguration);

        log.warn("[pathList] {}", result);
    }

    @Test()
    void pathListInexistent() {
        String path = "myNewFolderZZZ";
        inputConfiguration.getDataSet().setBlobPath(path);
        Object result = null;
        try {
            result = service.pathList(inputConfiguration);
            fail("The path should not exist");
        } catch (Exception e) {
        }
    }

    @Test
    void extractFolderPath() {
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

    @Test
    void pathExists() {
        // paths should exists
        String blobPath = "/myNewFolder/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertTrue(service.pathExists(inputConfiguration.getDataSet()));
        blobPath = "myNewFolder/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertTrue(service.pathExists(inputConfiguration.getDataSet()));
        blobPath = "myNewFolder/subfolder02/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertTrue(service.pathExists(inputConfiguration.getDataSet()));
        blobPath = "myNewFolder/subfolder02/subfolder03/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertTrue(service.pathExists(inputConfiguration.getDataSet()));
        // paths do not exist
        blobPath = "myNewFolder/subfolder03/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertFalse(service.pathExists(inputConfiguration.getDataSet()));
        blobPath = "/newFolder01ZZZ/blob.txt";
        inputConfiguration.getDataSet().setBlobPath(blobPath);
        assertFalse(service.pathExists(inputConfiguration.getDataSet()));
    }

    @Test
    @Ignore
    void pathReadUnknownFile() {
        String path = "myNewFolder/nostorelookup.java";
        inputConfiguration.getDataSet().setBlobPath(path);
        // inputConfiguration.getDataSet().setFormat(FileFormat.UNKNOWN);
        Iterator<Record> result = service.pathRead(inputConfiguration);
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
        }
    }

    @Test
    void pathReadParquetFile() {
        String path = "demo_gen2/in/parquet_file.parquet";
        inputConfiguration.getDataSet().setBlobPath(path);
        inputConfiguration.getDataSet().setFormat(FileFormat.PARQUET);
        Iterator<Record> result = service.pathRead(inputConfiguration);
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
            log.info("{} {}", r, r.getArray(String.class, "topics"));
        }
    }

    @Test
    void pathReadAvroFile() {
        // String path = "demo_gen2/in/users.avro";
        String path = "demo_gen2/in/userdata1.avro";
        inputConfiguration.getDataSet().setBlobPath(path);
        inputConfiguration.getDataSet().setFormat(FileFormat.AVRO);
        Iterator<Record> result = service.pathRead(inputConfiguration);
        // {"name": "Alyssa", "favorite_color": null, "favorite_numbers": [3, 9, 15, 20]}
        // {"type":"record","name":"User","namespace":"example.avro","fields":[{"name":"name","type":"string"},{"name":
        // "favorite_color","type":["string","null"]},{"name":"favorite_numbers","type":{"type":"array","items":
        // "int"}}]}

        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
            log.info("{}", r.getOptionalLong("cc"));
        }
    }

    @Test
    void pathReadMediumFile() {
        // String path = "myNewFolder/customer.csv";
        String path = "myNewFolder/customer_20190325.csv";
        inputConfiguration.getDataSet().setBlobPath(path);
        Iterator<Record> result = service.pathRead(inputConfiguration);
        int count = 0;
        while (result.hasNext()) {
            Record r = result.next();
            assertNotNull(r);
            count++;
        }
        assertEquals(10000, count);
    }

    @Test
    void pathUpdateFile() {
        String path = "myNewFolder/customer.csv";
        outputConfiguration.getDataSet().setBlobPath(path);
        outputConfiguration.setOverwrite(true);
        String content = "ABC;DEF;123;true;GBG\n";
        Object result = service.pathUpdate(outputConfiguration, content, 0);
        log.warn("[pathList] {}", result);
    }

    @Test
    void pathGetProperties() {
        String path = "myNewFolder/customer.csv";
        outputConfiguration.getDataSet().setBlobPath(path);
        Object result = service.pathGetProperties(outputConfiguration.getDataSet());
        log.warn("[pathGetProperties] {}", result);
    }
}
