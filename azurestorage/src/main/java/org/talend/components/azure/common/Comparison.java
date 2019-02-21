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
package org.talend.components.azure.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.storage.table.TableQuery;

public enum Comparison {
    EQUAL("EQUAL", TableQuery.QueryComparisons.EQUAL),

    NOT_EQUAL("NOT EQUAL", TableQuery.QueryComparisons.NOT_EQUAL),

    GREATER_THAN("GREATER THAN", TableQuery.QueryComparisons.GREATER_THAN),

    GREATER_THAN_OR_EQUAL("GREATER THAN OR EQUAL", TableQuery.QueryComparisons.GREATER_THAN_OR_EQUAL),

    LESS_THAN("LESS THAN", TableQuery.QueryComparisons.LESS_THAN),

    LESS_THAN_OR_EQUAL("LESS THAN OR EQUAL", TableQuery.QueryComparisons.LESS_THAN_OR_EQUAL);

    private String displayName;

    private String queryComparison;

    private static Map<String, Comparison> mapPossibleValues = new HashMap<>();

    static {
        Arrays.stream(values()).forEach(comparison -> mapPossibleValues.put(comparison.displayName, comparison));
    }

    Comparison(String displayName, String queryComparison) {
        this.displayName = displayName;
        this.queryComparison = queryComparison;
    }

    /**
     * Convert a function form String value to Azure Type {@link TableQuery.QueryComparisons}
     */
    public static String getQueryComparisons(String c) {
        if (!mapPossibleValues.containsKey(c)) {
            throw new IllegalArgumentException(String.format("Invalid value %s, it must be %s", c, mapPossibleValues));
        }
        return mapPossibleValues.get(c).queryComparison;
    }

    @Override
    public String toString() {
        return this.displayName;
    }
}
