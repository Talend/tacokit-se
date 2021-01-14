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
package org.talend.components.azure.runtime.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SchemaUtilsTest {

    @Test
    void correct() {
        Set<String> previous = new HashSet<>();
        String res = SchemaUtils.correct("Hello", 1, previous);
        Assertions.assertEquals("Hello", res);

        previous.add("Hello");
        res = SchemaUtils.correct("Hello", 1, previous);
        Assertions.assertEquals("Hello1", res);

        res = SchemaUtils.correct("2name?!special zz ", 1, previous);
        Assertions.assertEquals("_name__special_zz_", res);
        previous.add("_name__special_zz_");
        previous.add("_name__special_zz_1");
        previous.add("_name__special_zz_2");

        res = SchemaUtils.correct("2name?!special zz ", 1, previous);
        Assertions.assertEquals("_name__special_zz_3", res);

    }
}