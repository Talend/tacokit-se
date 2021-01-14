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
package org.talend.components.google.storage.service;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CredentialServiceTest {

    @Test
    void newStorage() throws IOException {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./engineering-test.json");

        final File fic = new File(resource.getPath());
        final String json = new String(Files.readAllBytes(fic.toPath()));

        final CredentialService credentialService = new CredentialService();
        final GoogleCredentials credentials = credentialService.getCredentials(json);
        Assertions.assertNotNull(credentials);

        final Storage storage = credentialService.newStorage(credentials);
        Assertions.assertNotNull(storage);
    }
}