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
package org.talend.components.adlsgen2.common.converter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RecordConverterTest {

    @Test
    void getCorrectSchemaFieldName() {

        Assertions.assertEquals("CA_HT", RecordConverter.getCorrectSchemaFieldName("CA HT", 0, Collections.emptySet()));

        Assertions.assertEquals("column___Name",
                RecordConverter.getCorrectSchemaFieldName("column?!^Name", 0, Collections.emptySet()));

        Assertions.assertEquals("P1_Vente_Qt_",
                RecordConverter.getCorrectSchemaFieldName("P1_Vente_Qté", 0, Collections.emptySet()));

    }

    @Test
    void getUniqueNameForSchemaField() {

        Assertions.assertEquals("Hello", RecordConverter.getUniqueNameForSchemaField("Hello", null));

        Assertions.assertEquals("Hello", RecordConverter.getUniqueNameForSchemaField("Hello", Collections.emptySet()));

        Set<String> previous = new HashSet<>();
        previous.add("Hello");
        Assertions.assertEquals("Hello1", RecordConverter.getUniqueNameForSchemaField("Hello", previous));

        previous.add("Hello1");
        Assertions.assertEquals("Hello2", RecordConverter.getUniqueNameForSchemaField("Hello", previous));

        previous.add("Hello2");
        Assertions.assertEquals("Hello3", RecordConverter.getUniqueNameForSchemaField("Hello", previous));

    }
}
