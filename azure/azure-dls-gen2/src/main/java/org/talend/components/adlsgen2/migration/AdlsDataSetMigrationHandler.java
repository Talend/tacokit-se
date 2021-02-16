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

import org.talend.sdk.component.api.component.MigrationHandler;

public class AdlsDataSetMigrationHandler implements MigrationHandler {

    private static final String DEFAULT_HEADER_SIZE = "1";

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        Map<String, String> migratedConfiguration = new HashMap<>(incomingData);
        if (incomingVersion < 2) {
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.recordDelimiter",
                    migratedConfiguration.remove("csvConfiguration.recordSeparator"));
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.useHeader",
                    migratedConfiguration.remove("csvConfiguration.header"));
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.header", DEFAULT_HEADER_SIZE);
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.encoding",
                    migratedConfiguration.remove("csvConfiguration.fileEncoding"));
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.fieldDelimiter",
                    migratedConfiguration.remove("csvConfiguration.fieldDelimiter"));
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.textEnclosureCharacter",
                    migratedConfiguration.remove("csvConfiguration.textEnclosureCharacter"));
            migratedConfiguration.put("csvConfiguration.csvFormatOptions.escapeCharacter",
                    migratedConfiguration.remove("csvConfiguration.escapeCharacter"));
        }
        return migratedConfiguration;
    }
}
