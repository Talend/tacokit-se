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
package org.talend.components.ftp.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileSystemEntry;
import org.mockftpserver.fake.filesystem.Permissions;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.talend.components.ftp.dataset.FTPDataSet;
import org.talend.components.ftp.datastore.FTPDataStore;
import org.talend.components.ftp.jupiter.FtpFile;
import org.talend.components.ftp.jupiter.FtpServer;
import org.talend.components.ftp.service.ftpclient.GenericFTPClient;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.junit5.WithComponents;

@FtpFile(base = "fakeFTP/", port = 4528)
@WithComponents(value = "org.talend.components.ftp")
public class FTPServiceTest {

    @Service
    Injector injector;

    private FTPDataSet dataset;

    private FTPService beanUnderTest;

    @BeforeEach
    public void init() {
        FTPDataStore datastore = new FTPDataStore();
        datastore.setHost("localhost");
        datastore.setUseCredentials(true);
        datastore.setUsername(FtpServer.USER);
        datastore.setPassword(FtpServer.PASSWD);
        datastore.setPort(4528);

        dataset = new FTPDataSet();
        dataset.setDatastore(datastore);
        dataset.setPath("/communes");

        beanUnderTest = new FTPService();
        injector.inject(beanUnderTest);
    }

    @Test
    public void testPathIsFile() {

        FakeConnectorConfiguration fakeConfig = new FakeConnectorConfiguration();
        fakeConfig.setDataSet(dataset);

        GenericFTPClient client = beanUnderTest.getClient(fakeConfig);

        Assertions.assertFalse(beanUnderTest.pathIsFile(client, dataset.getPath()), "/communes is not a file.");

        dataset.setPath("/communes/communes_0.csv");
        Assertions.assertTrue(beanUnderTest.pathIsFile(client, dataset.getPath()), "/communes/communes_0.csv is a file.");
    }

    @Test
    public void testConnection() {
        Assertions.assertEquals(HealthCheckStatus.Status.OK, beanUnderTest.validateDataStore(dataset.getDatastore()).getStatus(),
                "Status should be OK.");

        dataset.getDatastore().setPassword("WRONG");
        Assertions.assertEquals(HealthCheckStatus.Status.KO, beanUnderTest.validateDataStore(dataset.getDatastore()).getStatus(),
                "Status should be KO.");
    }

    // @Test
    public void testCheckWritePermission(UnixFakeFileSystem fs) {
        DirectoryEntry writable = new DirectoryEntry("/writable");
        writable.setPermissions(new Permissions("r--rw-rw-"));
        writable.setOwner("root");
        fs.add(writable);

        DirectoryEntry nonwritable = new DirectoryEntry("/nonwritable");
        nonwritable.setPermissions(new Permissions("r--r--r--"));
        nonwritable.setOwner("root");
        fs.add(nonwritable);

        FakeConnectorConfiguration fakeConfig = new FakeConnectorConfiguration();
        fakeConfig.setDataSet(dataset);

        dataset.setPath("/writable");
        Assertions.assertTrue(beanUnderTest.hasWritePermission(fakeConfig), "/writable folder should be writable");

        dataset.setPath("/writable/");
        Assertions.assertTrue(beanUnderTest.hasWritePermission(fakeConfig), "/writable/ folder should be writable");

        dataset.setPath("/nonwritable");
        Assertions.assertFalse(beanUnderTest.hasWritePermission(fakeConfig), "/nonwritable folder should not be writable");

        dataset.setPath("/nonwritable/");
        Assertions.assertFalse(beanUnderTest.hasWritePermission(fakeConfig), "/nonwritable/ folder should not be writable");
    }

}
