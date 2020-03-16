/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.runtime.model.search;

import lombok.ToString;

import java.util.List;

/**
 * Holds design-time specific information about search data model.
 */

@ToString
public class SearchInfo {

    /** Name of search data object type */
    private String typeName;

    /** List of search fields */
    private List<String> fields;

    public SearchInfo(String typeName, List<String> fields) {
        this.typeName = typeName;
        this.fields = fields;
    }

    public String getTypeName() {
        return typeName;
    }

    public List<String> getFields() {
        return fields;
    }

}
