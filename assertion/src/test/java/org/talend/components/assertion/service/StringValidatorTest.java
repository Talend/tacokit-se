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
package org.talend.components.assertion.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.assertion.conf.Config;

import static org.junit.jupiter.api.Assertions.*;

class StringValidatorTest {

    private StringValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new StringValidator();
    }

    @Test
    public void validate() {
        String expected = "expected";
        assertTrue(validator.validate(Config.Condition.EQUALS, expected, expected, null));
        assertTrue(validator.validate(Config.Condition.CONTAINS, expected, "azerty" + expected + "uiop", null));
        assertFalse(validator.validate(Config.Condition.EQUALS, expected, "azerty" + expected + "uiop", null));
        assertFalse(validator.validate(Config.Condition.CONTAINS, expected, "azertyuiop", null));
    }

}