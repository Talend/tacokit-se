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
package org.talend.components.salesforce.soql;

import java.util.List;

/**
 * Describes SOQL query field, contains
 * simple field name - name how it appears in Salesforce entity
 * full name - name which should be used in component avro schema
 * entity names - ordered list of Salesforce entities names to query to get field type
 */
public class FieldDescription {

    private String fullName;

    private String simpleName;

    private List<String> entityNames;

    public FieldDescription(String fullName, String simpleName, List<String> entityNames) {
        super();
        this.fullName = fullName;
        this.simpleName = simpleName;
        this.entityNames = entityNames;
    }

    public String getFullName() {
        return fullName;
    }

    public String getSimpleName() {
        return simpleName;
    }

    public List<String> getEntityNames() {
        return entityNames;
    }

}
