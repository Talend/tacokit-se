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
package org.talend.components.common.stream.output.parquet;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IndexValueTest {

    @Test
    void wrap() {
        final List<IndexValue<String>> collect = IndexValue.streamOf(Arrays.asList("1", "2", "3"))
                .map(IndexValue.wrap((String x) -> "_" + x)).collect(Collectors.toList());
        final IndexValue<String> indexValue1 = collect.get(0);
        Assertions.assertEquals(0, indexValue1.getIndex());
        Assertions.assertEquals("_1", indexValue1.getValue());

        final IndexValue<String> indexValue2 = collect.get(1);
        Assertions.assertEquals(1, indexValue2.getIndex());
        Assertions.assertEquals("_2", indexValue2.getValue());

        final IndexValue<String> indexValue3 = collect.get(2);
        Assertions.assertEquals(2, indexValue3.getIndex());
        Assertions.assertEquals("_3", indexValue3.getValue());
    }

}