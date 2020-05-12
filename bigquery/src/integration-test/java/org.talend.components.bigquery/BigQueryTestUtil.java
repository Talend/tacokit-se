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
package org.talend.components.bigquery;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.talend.components.bigquery.datastore.BigQueryConnection;
import org.talend.sdk.component.maven.MavenDecrypter;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Slf4j
public class BigQueryTestUtil {

    public static String GOOGLE_APPLICATION_CREDENTIALS;

    public static String GOOGLE_PROJECT;

    static {
        GOOGLE_APPLICATION_CREDENTIALS = Optional.ofNullable(System.getProperty("GOOGLE_APPLICATION_CREDENTIALS"))
                .orElseGet(() -> Optional.ofNullable(System.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
                        .orElseGet(() -> Optional.ofNullable(new MavenDecrypter().find("GOOGLE_APPLICATION_CREDENTIALS").getPassword())
                            .orElseThrow(() -> new RuntimeException("GOOGLE_APPLICATION_CREDENTIALS not set"))));

        GOOGLE_PROJECT = Optional.ofNullable(System.getProperty("GOOGLE_PROJECT")).orElse("engineering-152721");
        log.info("Using " + GOOGLE_APPLICATION_CREDENTIALS + " as Google credentials file");
    }

    public static BigQueryConnection getConnection() {
        String jsonCredentials = "";
        try (FileInputStream in = new FileInputStream(GOOGLE_APPLICATION_CREDENTIALS);
                BufferedInputStream bIn = new BufferedInputStream(in)) {
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = bIn.read(buffer)) > 0) {
                jsonCredentials += new String(buffer, 0, read, StandardCharsets.UTF_8);
            }
            jsonCredentials = jsonCredentials.replace("\n", " ").trim();
        } catch (IOException ioe) {
            Assertions.fail(ioe);
        }

        BigQueryConnection connection = new BigQueryConnection();
        connection.setProjectName(GOOGLE_PROJECT);
        connection.setJsonCredentials(jsonCredentials);

        return connection;
    }
}
