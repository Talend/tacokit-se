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

class DateValidatorTest {

    private DateValidator validator;

    private String expected;

    @BeforeEach
    public void beforeEach() {
        validator = new DateValidator();
        expected = "2020/10/01 00:06:00+02";
    }

    @Test
    public void valideStringEquals() {
        String value = "2020/10/01 00:06:00+02";
        assertTrue(validator.validate(Config.Condition.EQUALS, expected, value, null));
        assertFalse(validator.validate(Config.Condition.INFERIOR, expected, value, null));
        assertFalse(validator.validate(Config.Condition.SUPERIOR, expected, value, null));
    }

    @Test
    public void valideStringSuperior() {
        String value = "2020/10/01 00:06:01+02";
        assertFalse(validator.validate(Config.Condition.EQUALS, expected, value, null));
        assertFalse(validator.validate(Config.Condition.INFERIOR, expected, value, null));
        assertTrue(validator.validate(Config.Condition.SUPERIOR, expected, value, null));
    }

    @Test
    public void valideStringInferior() {
        String value = "2020/10/01 00:05:59+02";
        assertFalse(validator.validate(Config.Condition.EQUALS, expected, value, null));
        assertTrue(validator.validate(Config.Condition.INFERIOR, expected, value, null));
        assertFalse(validator.validate(Config.Condition.SUPERIOR, expected, value, null));
    }

    @Test
    public void valideWithFormat() {
        String format = "dd-MM-yyyy '|' ss:HH:mmX";
        expected = "01-10-2020 | 05:00:59Z";
        String value = "30-09-2020 | 05:00:59Z";

        validator.setFormat(format);
        assertFalse(validator.validate(Config.Condition.EQUALS, expected, value, null));
        assertTrue(validator.validate(Config.Condition.INFERIOR, expected, value, null));
        assertFalse(validator.validate(Config.Condition.SUPERIOR, expected, value, null));
    }
}