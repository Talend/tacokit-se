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
package org.talend.components.adlsgen2.migration;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AdlsDataSetMigrationHandlerTest {

    @Test
    public void testMigrationAfterRefactoringVersion1() {
        AdlsDataSetMigrationHandler migrator = new AdlsDataSetMigrationHandler();

        Map<String, String> oldConfigMap = new HashMap<>();

        oldConfigMap.put("csvConfiguration.fileEncoding", "UTF8");
        oldConfigMap.put("csvConfiguration.header", "true");
        oldConfigMap.put("csvConfiguration.fieldDelimiter", "SEMICOLON");
        oldConfigMap.put("csvConfiguration.recordSeparator", "CRLF");
        oldConfigMap.put("csvConfiguration.textEnclosureCharacter", "\\");
        oldConfigMap.put("csvConfiguration.escapeCharacter", "\"");
        oldConfigMap.put("csvConfiguration.csvSchema", "a;b;c");
        oldConfigMap.put("format", "CSV");
        oldConfigMap.put("connection.endpointSuffix", "dfs.core.windows.net"); // no need to add another connection properties

        Map<String, String> migratedConfigMap = migrator.migrate(1, oldConfigMap);

        Assertions.assertEquals(migratedConfigMap.size(), oldConfigMap.size() + 1,
                "Size of migrated configuration should be old config + 1");
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.header"),
                migratedConfigMap.get("csvConfiguration.csvFormatOptions.useHeader"));
        Assertions.assertEquals("1", migratedConfigMap.get("csvConfiguration.csvFormatOptions.header"));
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.recordSeparator"),
                migratedConfigMap.get("csvConfiguration.csvFormatOptions.recordDelimiter"));
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.fieldDelimiter"),
                migratedConfigMap.get("csvConfiguration.csvFormatOptions.fieldDelimiter"));
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.textEnclosureCharacter"),
                migratedConfigMap.get("csvConfiguration.csvFormatOptions.textEnclosureCharacter"));
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.escapeCharacter"),
                migratedConfigMap.get("csvConfiguration.csvFormatOptions.escapeCharacter"));
        Assertions.assertEquals(oldConfigMap.get("csvConfiguration.schema"), migratedConfigMap.get("csvConfiguration.schema"));
        Assertions.assertEquals(oldConfigMap.get("format"), migratedConfigMap.get("format"));
        Assertions.assertEquals(oldConfigMap.get("connection.endpointSuffix"),
                migratedConfigMap.get("connection.endpointSuffix"));
    }
}