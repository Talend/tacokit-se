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

import lombok.AllArgsConstructor;

/**
 * Type of NetSuite reference.
 */
@AllArgsConstructor
public enum RefType {
    RECORD_REF("RecordRef"),
    CUSTOM_RECORD_REF("CustomRecordRef"),
    CUSTOMIZATION_REF("CustomizationRef");

    /** Short name of NetSuite native reference object type. */
    private String typeName;

    public String getTypeName() {
        return typeName;
    }

    /**
     * Get reference type enum constant by name of reference data object type.
     *
     * @param typeName reference data object type
     * @return reference type enum constant
     * @throws IllegalArgumentException if type name don't match known any type name
     */
    public static RefType getByTypeName(String typeName) {
        for (RefType value : values()) {
            if (value.typeName.equals(typeName)) {
                return value;
            }
        }
        throw new IllegalArgumentException("Invalid type name: " + typeName);
    }
}
