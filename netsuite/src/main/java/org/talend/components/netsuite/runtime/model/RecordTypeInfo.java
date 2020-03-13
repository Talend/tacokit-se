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
import lombok.Data;
import lombok.ToString;

/**
 * Holds information about record type.
 */

@Data
@ToString
@AllArgsConstructor
public class RecordTypeInfo {

    /**
     * Name identifying record type.
     *
     * <p>
     * For standard record types name is the same as {@link RecordTypeDesc#getTypeName()}.
     */
    protected String name;

    /** Record type descriptor. */
    protected RecordTypeDesc recordType;

    /**
     * Create instance of record type info using given record type descriptor.
     *
     * @param recordType record type descriptor
     */
    public RecordTypeInfo(RecordTypeDesc recordType) {
        this.name = recordType.getTypeName();
        this.recordType = recordType;
    }

    /**
     * Get display name of record type.
     *
     * @return display name
     */
    public String getDisplayName() {
        return recordType.getTypeName();
    }

    /**
     * Get type of reference for this record type.
     *
     * @return type of reference
     */
    public RefType getRefType() {
        return RefType.RECORD_REF;
    }
}
