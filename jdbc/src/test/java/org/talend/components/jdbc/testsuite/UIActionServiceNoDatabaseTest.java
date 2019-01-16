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
package org.talend.components.jdbc.testsuite;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.stream.Stream;

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.*;

@DisplayName("UIActionService")
@WithComponents("org.talend.components.jdbc")
class UIActionServiceNoDatabaseTest {

    @Service
    private UIActionService myService;

    @Test
    @DisplayName("DynamicValue - Load Drivers")
    void loadSupportedDataBaseTypes() {
        final Values values = myService.loadSupportedDataBaseTypes();
        assertNotNull(values);
        assertFalse(values.getItems().isEmpty());
        assertTrue(values.getItems().stream().map(Values.Item::getId).collect(toSet()).containsAll(Stream
                .of("MySQL", "Derby", "Oracle", "Snowflake", "PostgreSQL", "Redshift", "MariaDB", "MSSQL").collect(toSet())));
    }

    @Test
    @DisplayName("Query - Validate select query")
    void validateReadOnlyQuery() {
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("update table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("delete table").getStatus());
        assertEquals(ValidationResult.Status.KO, myService.validateReadOnlySQLQuery("insert table").getStatus());
        assertEquals(ValidationResult.Status.KO,
                myService.validateReadOnlySQLQuery("some other command other than select").getStatus());
        assertEquals(ValidationResult.Status.OK, myService.validateReadOnlySQLQuery("select * ").getStatus());
    }

}
