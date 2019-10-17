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
package org.talend.components.common.text;

import org.junit.Assert;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

class SubstitutorTest {

    @ParameterizedTest
    @CsvSource(value = { "${,},This is a simple ${place_holder}.,This is a simple P_L_A_C_E_H_O_L_D_E_R.",
            "((,)),A more ((complex)) example with ((two)) place holders.,A more C_O_M_P_L_E_X example with T_W_O place holders.",
            "${,},Example ${three} with \\${escape} ${place_holder}.,Example 3 with ${escape} P_L_A_C_E_H_O_L_D_E_R.",
            "[,],[one] key with [unknown:-MY_DEFAULT] value,O_N_E key with MY_DEFAULT value",
            "${,},${one} ${two} ${three},O_N_E T_W_O 3", "${,},${aaa:-AAA} ${bbb:-BBB} ${ccc:-CCC},AAA BBB CCC" })
    void testSubstitutor(final String prefix, final String suffix, final String value, final String expected) {
        final Map<String, String> store = new HashMap<>();
        store.put("place_holder", "P_L_A_C_E_H_O_L_D_E_R");
        store.put("complex", "C_O_M_P_L_E_X");
        store.put("one", "O_N_E");
        store.put("two", "T_W_O");
        store.put("escape", "E_S_C_A_P_E");
        store.put("three", "3");

        final Substitutor substitutor = new Substitutor(prefix, suffix, new Function<String, String>() {

            @Override
            public String apply(final String key) {
                return store.get(key);
            }
        });

        final String transformed = substitutor.replace(value);
        Assert.assertEquals(expected, transformed);

    }

}