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
package org.talend.components.migration.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DSEMigrationHandlerTest {

    private Map<String, String> incomingData;

    @BeforeEach
    void buildIncomingData() {
        incomingData = new HashMap<>();
        incomingData.put("dso.dso_legacy", "legacy data");
        incomingData.put("dso.dso_migration_handler_callback", "");
        incomingData.put("dse_legacy", "legacy data");
        incomingData.put("dse_migration_handler_callback", "");
        incomingData.put("dse_incoming", "");
        incomingData.put("dse_outgoing", "");
    }

    @Test
    void testDSEMigration() {
        DatasetMigrationHandler dsmh = new DatasetMigrationHandler();
        final Map<String, String> migrated = dsmh.migrate(1, incomingData);

        assertEquals(migrated.get("dse_legacy"), "legacy data");
        assertEquals(migrated.get("dse_duplication"), "legacy data");
        assertFalse(migrated.get("dse_migration_handler_callback").isEmpty());

        assertEquals(migrated.get("dse_incoming"),
                "{\n" + "\t\"dse_incoming\" : \"\",\n" + "\t\"dse_outgoing\" : \"\",\n"
                        + "\t\"dso.dso_migration_handler_callback\" : \"\",\n" + "\t\"dse_legacy\" : \"legacy data\",\n"
                        + "\t\"dse_migration_handler_callback\" : \"\",\n" + "\t\"dso.dso_legacy\" : \"legacy data\"\n" + "}");
        assertFalse(migrated.get("dse_outgoing").isEmpty());

        assertEquals(migrated.get("dso.dso_legacy"), "legacy data");
        assertNull(migrated.get("dso.dso_duplication"));
        assertTrue(migrated.get("dso.dso_migration_handler_callback").isEmpty());
    }

}