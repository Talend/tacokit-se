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
package org.talend.components.migration.demo.handlers;

import org.talend.sdk.component.api.component.MigrationHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion, Map<String, String> incomingData) {
        Map<String, String> migrated = new HashMap<>(incomingData);

        migrated.remove("date_format");
        migrated.put("mode", "TABLE");

        migrated.put("table.name", incomingData.get("table"));
        migrated.remove("table");

        migrated.remove("columns");
        migrated.put("table.columns", incomingData.get("columns"));

        return migrated;
    }
}
