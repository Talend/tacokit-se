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
package org.talend.components.migration.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SinkMigrationHandlerTest {

    private Map<String, String> incomingData;

    @BeforeEach
    void buildIncomingData() {
        incomingData = new HashMap<>();
        incomingData.put("configuration.dse.dso.dso_legacy", "legacy data");
        incomingData.put("configuration.dse.dso.dso_migration_handler_callback", "");
        incomingData.put("configuration.dse.dse_legacy", "legacy data");
        incomingData.put("configuration.dse.dse_migration_handler_callback", "");
        incomingData.put("configuration.legacy", "legacy data");
        incomingData.put("configuration.sink_migration_handler_callback", "");
    }

    @Test
    void testSourceMigration() {
        SinkMigrationHandler dsmh = new SinkMigrationHandler();
        final Map<String, String> migrated = dsmh.migrate(1, incomingData);

        assertEquals(migrated.get("configuration.dse.dse_legacy"), "legacy data");
        assertNull(migrated.get("configuration.dse.dse_duplication"));
        assertTrue(migrated.get("configuration.dse.dse_migration_handler_callback").isEmpty());

        assertEquals(migrated.get("configuration.dse.dso.dso_legacy"), "legacy data");
        assertNull(migrated.get("configuration.dse.dso.dso_duplication"));
        assertTrue(migrated.get("configuration.dse.dso.dso_migration_handler_callback").isEmpty());

        assertEquals(migrated.get("configuration.legacy"), "legacy data");
        assertEquals(migrated.get("configuration.duplication"), "legacy data");
        assertFalse(migrated.get("configuration.sink_migration_handler_callback").isEmpty());
    }

}