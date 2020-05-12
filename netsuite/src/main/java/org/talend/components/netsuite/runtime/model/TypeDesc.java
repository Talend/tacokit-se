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
package org.talend.components.netsuite.runtime.model;

import lombok.ToString;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Descriptor of NetSuite data object model type.
 */

@ToString
public class TypeDesc {

    /** Short name of data object type. */
    private String typeName;

    /** Class of data object type. */
    private Class<?> typeClass;

    /** List of field descriptors for this data object type. */
    private List<FieldDesc> fields;

    /** Map of field descriptors by names, for faster access. */
    private Map<String, FieldDesc> fieldMap;

    public TypeDesc(String typeName, Class<?> typeClass, List<FieldDesc> fields) {
        this.typeName = typeName;
        this.typeClass = typeClass;
        this.fields = fields;

        fieldMap = fields.stream().collect(Collectors.toMap(FieldDesc::getName, fieldDesc -> fieldDesc));
    }

    public String getTypeName() {
        return typeName;
    }

    public Class<?> getTypeClass() {
        return typeClass;
    }

    public FieldDesc getField(String name) {
        return fieldMap.get(name);
    }

    public Map<String, FieldDesc> getFieldMap() {
        return Collections.unmodifiableMap(fieldMap);
    }

    public List<FieldDesc> getFields() {
        return Collections.unmodifiableList(fields);
    }
}
