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
package org.talend.components.azure.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.Comparison;

import static org.junit.Assert.assertEquals;

public class ComparisonTest {

    @Test
    public void testGetQueryComparison() {
        String expectedQueryComparison = "eq";
        String queryComparison = Comparison.getQueryComparisons(Comparison.EQUAL.toString());

        assertEquals(expectedQueryComparison, queryComparison);
    }

    @Test
    public void verifyGetQueryComparisonThrowAnException() {
        String illegalValue = "abc";
        Assertions.assertThrows(RuntimeException.class, () -> Comparison.getQueryComparisons(illegalValue));
    }
}