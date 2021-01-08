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
package org.talend.components.common.stream.format.fixed;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FixedConfigurationTest {

    @ParameterizedTest
    @ValueSource(strings = { "125", "124;45;44" })
    void isValidTrue(String chaine) {
        final FixedConfiguration configuration = new FixedConfiguration();
        configuration.setLengthFields(chaine);
        Assertions.assertTrue(configuration.isValid(), chaine + " not valid");
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "12;", "11;;22", ";", "Hi" })
    void isValidFalse(String chaine) {
        final FixedConfiguration configuration = new FixedConfiguration();
        configuration.setLengthFields(chaine);
        Assertions.assertFalse(configuration.isValid(), chaine + " valid");
    }

    @Test
    void getLengthFields() {
        final FixedConfiguration configuration = new FixedConfiguration();
        configuration.setLengthFields("124;45;44");
        final int[] realLengthFields = configuration.getRealLengthFields();
        Assertions.assertEquals(3, realLengthFields.length);

        Assertions.assertEquals(124, realLengthFields[0]);
        Assertions.assertEquals(45, realLengthFields[1]);
        Assertions.assertEquals(44, realLengthFields[2]);
    }
}