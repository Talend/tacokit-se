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

class BytesValidatorTest {

    private BytesValidator validator;

    @BeforeEach
    public void beforeEach() {
        validator = new BytesValidator();
    }

    @Test
    public void validate() {
        String sbytearr = "000102030A7F80FFFEFDF6"; // [0, 1, 2, 3, 10, 127, -128, -1, -2, -3, -10]
        final byte[] bytearr = Util.hexStringToByteArray(sbytearr);

        assertTrue(validator.validate(Config.Condition.EQUALS, sbytearr, bytearr, null));
        assertTrue(validator.validate(Config.Condition.CONTAINS, "0A7F80", bytearr, null));
        assertTrue(validator.validate(Config.Condition.CONTAINS, "80FFFE", bytearr, null));

        assertFalse(validator.validate(Config.Condition.EQUALS, "040302", new byte[] { 5, 4, 3, 2, 1 }, null));
        assertFalse(validator.validate(Config.Condition.CONTAINS, "060403", new byte[] { 5, 4, 3, 2, 1 }, null));
    }

}