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
import org.talend.components.migration.conf.AbstractConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class AbstractTestMigrationHandlerTest {

    private Map<String, String> incomingData;

    @BeforeEach
    void buildIncomingData() {
        incomingData = new HashMap<>();
        incomingData.put("configuration.test_callback", "");
        incomingData.put("configuration.level1.level2.legacy", "legacy data");
        incomingData.put("configuration.level1.level2.duplication", "");
        incomingData.put("configuration.incoming_conf", "");
        incomingData.put("configuration.outgoing_conf", "");
    }

    @Test
    void testMigrate() {
        final TestMigrationHandler testMigrationHandler = new TestMigrationHandler();
        final Map<String, String> migrated = testMigrationHandler.migrate(1, incomingData);
        assertEquals(migrated.get("configuration.level1.level2.legacy"), "legacy data");
        assertEquals(migrated.get("configuration.level1.level2.duplication"), "legacy data");

        final String callback = migrated.get("configuration.test_callback");
        final String[] split = callback.split("\\|");
        final String fromTo = split[0].trim();
        final String sDate = split[1].trim();

        assertEquals(fromTo, "1 -> " + AbstractConfig.VERSION);

        // Check if date has format : yyyy/MM/dd HH:mm:ss
        Pattern p = Pattern.compile("[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");
        Matcher m = p.matcher(sDate);
        assertTrue(m.matches());

        final String incoming = "{\n" + "\t\"configuration.level1.level2.legacy\" : \"legacy data\",\n"
                + "\t\"configuration.outgoing_conf\" : \"\",\n" + "\t\"configuration.incoming_conf\" : \"\",\n"
                + "\t\"configuration.level1.level2.duplication\" : \"\",\n" + "\t\"configuration.test_callback\" : \"\"\n" + "}";
        assertEquals(migrated.get("configuration.incoming_conf"), incoming);

        final String outgoing = "{\n" + "\t\"configuration.level1.level2.legacy\" : \"legacy data\",\n"
                + "\t\"configuration.outgoing_conf\" : \"\",\n" + "\t\"configuration.incoming_conf\" : \"{\n"
                + "\t\"configuration.level1.level2.legacy\" : \"legacy data\",\n" + "\t\"configuration.outgoing_conf\" : \"\",\n"
                + "\t\"configuration.incoming_conf\" : \"\",\n" + "\t\"configuration.level1.level2.duplication\" : \"\",\n"
                + "\t\"configuration.test_callback\" : \"\"\n" + "}\",\n"
                + "\t\"configuration.level1.level2.duplication\" : \"legacy data\",\n"
                + "\t\"configuration.test_callback\" : \"1 -> " + AbstractConfig.VERSION + " | " + sDate + "\"\n" + "}";
        assertEquals(migrated.get("configuration.outgoing_conf"), outgoing);
    }

    private final static class TestMigrationHandler extends AbstractTestMigrationHandler {

        @Override
        public String getPrefix() {
            return "configuration.";
        }

        @Override
        public String getCallbackPropertyName() {
            return "test_callback";
        }

        @Override
        protected String getDuplication() {
            return "level1.level2.duplication";
        }

        @Override
        protected String getLegacy() {
            return "level1.level2.legacy";
        }

        @Override
        protected String getIncoming() {
            return "incoming_conf";
        }

        @Override
        protected String getOutgoing() {
            return "outgoing_conf";
        }

        @Override
        protected void from(Map<String, String> incomingData) {

        }
    }
}