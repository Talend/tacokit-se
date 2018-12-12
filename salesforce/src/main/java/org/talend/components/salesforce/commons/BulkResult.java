
/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.commons;

import java.util.Map;
import java.util.TreeMap;

public class BulkResult {

    Map<String, String> values;

    public BulkResult() {
        values = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    public void setValue(String field, String vlaue) {
        values.put(field, vlaue);
    }

    public Object getValue(String fieldName) {
        return values.get(fieldName);
    }

    public void copyValues(BulkResult result) {
        if (result == null) {
            return;
        } else {
            for (String key : result.values.keySet()) {
                String value = result.values.get(key);
                if ("#N/A".equals(value)) {
                    value = null;
                }
                values.put(key, value);
            }
        }
    }
}