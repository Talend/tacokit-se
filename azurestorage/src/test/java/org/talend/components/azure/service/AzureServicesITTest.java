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
package org.talend.components.azure.service;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.AzureConnection;
import org.talend.components.azure.common.AzureTableConnection;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@WithComponents("org.talend.components.azure")
public class AzureServicesITTest {

    @Service
    private AzureComponentServices componentServices;

    private static AzureTableConnection dataSet;

    private static Server account;

    private boolean isCredentialsBroken = false;

    @BeforeAll
    public static void init() {
        dataSet = new AzureTableConnection();
        AzureConnection dataStore = new AzureConnection();
        final MavenDecrypter decrypter = new MavenDecrypter();
        account = decrypter.find("azure.account");
        dataStore.setAccountName(account.getUsername());
        dataStore.setAccountKey(account.getPassword());

        dataSet.setConnection(dataStore);
    }

    @AfterEach
    public void recover() {
        if (isCredentialsBroken) {
            dataSet.getConnection().setAccountName(account.getUsername());
            dataSet.getConnection().setAccountKey(account.getPassword());
            dataSet.getConnection().setUseAzureSharedSignature(false);
        }
    }

    @Test
    public void testHealthCheckOK() {
        assertEquals(HealthCheckStatus.Status.OK, componentServices.testConnection(dataSet.getConnection()).getStatus());
    }

    @Test
    public void testHealthCheckFailing() {
        String notExistingAccountName = "testNotExistingAccountName";
        dataSet.getConnection().setAccountName(notExistingAccountName);
        isCredentialsBroken = true;
        assertEquals(HealthCheckStatus.Status.KO, componentServices.testConnection(dataSet.getConnection()).getStatus());
    }

    @Test
    public void testGetTableNamesIsNotEmpty() {
        assertFalse(componentServices.getTableNames(dataSet.getConnection()).getItems().isEmpty());
    }

    @Test
    public void testGetSchema() {
        // TODO create table with some schema before test
        String tableName = "mytable";
        dataSet.setTableName(tableName);
        Schema schema = componentServices.guessSchema(dataSet);

        assertTrue(schema.getEntries().size() >= 3);
        // TODO drop table after test
    }

    @Test
    public void testGetSchemaFailing() {
        String notExistingTableName = "notExistingTable";

        dataSet.setTableName(notExistingTableName);
        Assertions.assertThrows(RuntimeException.class, () -> componentServices.guessSchema(dataSet));
    }

}
