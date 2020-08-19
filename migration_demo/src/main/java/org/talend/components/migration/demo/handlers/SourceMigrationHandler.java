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

public class SourceMigrationHandler implements MigrationHandler {

    @Override
    public Map<String, String> migrate(int incomingVersion,
                                       Map<String, String> incomingData) {
        Map<String, String> migrated = new HashMap<>(incomingData);

        migrated.put("configuration.log_level",
                incomingData.get("configuration.log_levle"));
        migrated.remove("configuration.log_levle");
        migrated.put("configuration.date_format",
                incomingData.get("configuration.dse.date_format"));
        return migrated;
    }
}
