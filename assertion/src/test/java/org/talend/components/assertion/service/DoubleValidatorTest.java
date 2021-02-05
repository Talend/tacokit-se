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

class DoubleValidatorTest {

    private NumberValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new DoubleValidator();
    }

    @Test
    public void validateEquals() {
        String expected = "12345.2589";

        assertTrue(validator.validate(Config.Condition.EQUALS, expected, 12345.2589d, null));
        // assertTrue(validator.validate(Config.Condition.EQUALS, expected, 12345.2589f)); // float precision error
        assertFalse(validator.validate(Config.Condition.EQUALS, expected, 12345.2589d - 0.0001d, null));
    }

    @Test
    public void validateInferior() {
        String expected = "12345.2589";

        assertTrue(validator.validate(Config.Condition.INFERIOR, expected, 12345.2589d - 0.0001d, null));
        assertFalse(validator.validate(Config.Condition.INFERIOR, expected, 12345.2589d + 0.0001d, null));
    }

    @Test
    public void validateSuperior() {
        String expected = "12345.2589";

        assertTrue(validator.validate(Config.Condition.SUPERIOR, expected, 12345.2589d + 0.0001d, null));
        assertFalse(validator.validate(Config.Condition.SUPERIOR, expected, 12345.2589d - 0.0001d, null));

    }

}