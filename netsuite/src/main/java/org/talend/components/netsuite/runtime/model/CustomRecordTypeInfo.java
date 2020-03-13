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

import org.talend.components.netsuite.runtime.client.NsRef;

import lombok.ToString;

/**
 * Holds information about custom record type.
 */

@ToString
public class CustomRecordTypeInfo extends RecordTypeInfo {

    /** Customization ref for this record type. */
    private NsRef customizationRef;

    /**
     * Create instance of custom record type info using name, record type descriptor and
     * customization ref.
     *
     * @param name name of custom record type
     * @param recordType record type descriptor
     * @param customizationRef customization ref
     */
    public CustomRecordTypeInfo(String name, RecordTypeDesc recordType, NsRef customizationRef) {
        super(name, recordType);
        this.customizationRef = customizationRef;
    }

    @Override
    public String getDisplayName() {
        return customizationRef.getName();
    }

    /**
     * Get customization ref.
     *
     * @return customization ref
     */
    public NsRef getCustomizationRef() {
        return customizationRef;
    }

    @Override
    public RefType getRefType() {
        return RefType.CUSTOM_RECORD_REF;
    }
}
