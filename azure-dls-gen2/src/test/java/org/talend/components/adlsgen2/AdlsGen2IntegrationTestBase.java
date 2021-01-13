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
package org.talend.components.adlsgen2;

import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.jupiter.api.BeforeEach;
import org.talend.sdk.component.api.DecryptedServer;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.WithMavenServers;
import org.talend.sdk.component.maven.Server;

@WithComponents("org.talend.components.adlsgen2")
@WithMavenServers
public class AdlsGen2IntegrationTestBase extends AdlsGen2TestBase {

    @DecryptedServer("azure-dls-gen2.storage")
    private Server mvnStorage;

    @DecryptedServer("azure-dls-gen2.sas")
    private Server mvnAccountSAS;

    @DecryptedServer("azure-dls-gen2.sharedkey")
    private Server mvnAccountSharedKey;

    protected static String accountName;

    protected static String storageFs;

    protected static String accountKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX==";

    protected static String sas;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();

        accountName = mvnAccountSAS.getUsername();
        storageFs = mvnStorage.getUsername();
        accountKey = mvnAccountSharedKey.getPassword();
        sas = mvnAccountSAS.getPassword();

        Assume.assumeThat(accountName, Matchers.not("username"));
        Assume.assumeThat(sas, Matchers.not("password"));

        connection.setAccountName(accountName);
        connection.setSharedKey(accountKey);
        connection.setSas(sas);

        dataSet.setFilesystem(storageFs);
    }
}
