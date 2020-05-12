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
package org.talend.components.netsuite.runtime.client;

import org.talend.components.netsuite.runtime.NsObjectTransducer;
import org.talend.components.netsuite.runtime.model.RefType;
import org.talend.components.netsuite.runtime.model.beans.BeanInfo;
import org.talend.components.netsuite.runtime.model.beans.Beans;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * Holds information about NetSuite's reference.
 *
 * <p>
 * NetSuite data model uses different data object for each type of reference.
 * The {@code NsRef} combines fields of all types of references, type of reference is specified
 * by {@link #refType} field.
 *
 * <p>
 * Supported reference types:
 * <ul>
 * <li>{@code RecordRef}</li>
 * <li>{@code CustomRecordRef}</li>
 * <li>{@code CustomizationRef}</li>
 * </ul>
 */

@Data
@NoArgsConstructor
@ToString
@EqualsAndHashCode
public class NsRef {

    /** Type of reference. */
    private RefType refType;

    /** Name of a referenced object. Can be {@code null}. */
    private String name;

    /** Name of record type. Can be {@code null}. */
    private String type;

    /** Internal ID of a referenced object. */
    private String internalId;

    /** External ID of a referenced object. */
    private String externalId;

    /** Script ID of a referenced object. */
    private String scriptId;

    /** Identifier of a referenced object's type. */
    private String typeId;

    public NsRef(RefType refType) {
        this.refType = refType;
    }

    /**
     * Create ref object from NetSuite's native ref data object.
     *
     * @param ref native ref data object
     * @return ref object
     */
    @SuppressWarnings("unchecked")
    public static NsRef fromNativeRef(Object ref) {
        String typeName = ref.getClass().getSimpleName();
        RefType refType = RefType.getByTypeName(typeName);
        NsRef nsRef = new NsRef();
        nsRef.setRefType(refType);
        BeanInfo beanInfo = Beans.getBeanInfo(ref.getClass());
        nsRef.setInternalId((String) Beans.getSimpleProperty(ref, NsObjectTransducer.INTERNAL_ID));
        nsRef.setExternalId((String) Beans.getSimpleProperty(ref, NsObjectTransducer.EXTERNAL_ID));
        if (refType == RefType.RECORD_REF) {
            nsRef.setType(Beans.getEnumAccessor((Class<Enum<?>>) beanInfo.getProperty("type").getReadType())
                    .getStringValue((Enum<?>) Beans.getSimpleProperty(ref, "type")));
        } else if (refType == RefType.CUSTOM_RECORD_REF) {
            nsRef.setTypeId((String) Beans.getSimpleProperty(ref, "typeId"));
        } else if (refType == RefType.CUSTOMIZATION_REF) {
            nsRef.setScriptId((String) Beans.getSimpleProperty(ref, "scriptId"));
        }
        return nsRef;
    }

}