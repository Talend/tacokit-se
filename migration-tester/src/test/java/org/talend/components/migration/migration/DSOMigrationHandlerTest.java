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

import static org.junit.jupiter.api.Assertions.*;

class DSOMigrationHandlerTest {

    private Map<String, String> incomingData;

    @BeforeEach
    void buildIncomingData() {
        incomingData = new HashMap<>();
        incomingData.put("dso_legacy", "legacy data");
        incomingData.put("dso_migration_handler_callback", "");
        incomingData.put("dso_incoming", "");
        incomingData.put("dso_outgoing", "");
    }

    @Test
    void testDSOMigration() {
        DatastoreMigrationHandler dsmh = new DatastoreMigrationHandler();
        final Map<String, String> migrated = dsmh.migrate(1, incomingData);

        assertEquals(migrated.get("dso_legacy"), "legacy data");
        assertEquals(migrated.get("dso_duplication"), "legacy data");
        assertFalse(migrated.get("dso_migration_handler_callback").isEmpty());
        assertEquals(migrated.get("dso_incoming"),
                "{\n" + "\t\"dso_outgoing\" : \"\",\n" + "\t\"dso_legacy\" : \"legacy data\",\n" + "\t\"dso_incoming\" : \"\",\n"
                        + "\t\"dso_migration_handler_callback\" : \"\"\n" + "}");
        assertFalse(migrated.get("dso_outgoing").isEmpty());

        assertNotNull(migrated.get("dso_shouldNotBeEmpty"));
        assertFalse(migrated.get("dso_shouldNotBeEmpty").isEmpty());
    }

}