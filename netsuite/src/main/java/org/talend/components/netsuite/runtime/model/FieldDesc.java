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

import lombok.Data;

import javax.xml.datatype.XMLGregorianCalendar;
import java.time.ZonedDateTime;

/**
 * Descriptor of data object type's field.
 *
 * @see TypeDesc
 * @see SimpleFieldDesc
 * @see CustomFieldDesc
 */
@Data
public abstract class FieldDesc {

    /** Name of field. */
    protected String name;

    /** Class of value stored by this field. */
    protected Class<?> valueType;

    /** Specifies whether this is key field. */
    protected boolean key;

    /** Specifies whether this field can accept {@code null} as value. */
    protected boolean nullable;

    /** Length of this field. */
    protected int length;

    /**
     * Get this field as {@link SimpleFieldDesc}.
     *
     * @return this field as {@link SimpleFieldDesc}
     */
    public SimpleFieldDesc asSimple() {
        return (SimpleFieldDesc) this;
    }

    /**
     * Get this field as {@link CustomFieldDesc}.
     *
     * @return this field as {@link CustomFieldDesc}
     */
    public CustomFieldDesc asCustom() {
        return (CustomFieldDesc) this;
    }

    public Class<?> getRecordValueType() {
        if (valueType == Boolean.TYPE || valueType == Boolean.class) {
            return Boolean.class;
        } else if (valueType == Integer.TYPE || valueType == Integer.class) {
            return Integer.class;
        } else if (valueType == Long.TYPE || valueType == Long.class) {
            return Long.class;
        } else if (valueType == Double.TYPE || valueType == Double.class) {
            return Double.class;
        } else if (valueType == Float.TYPE || valueType == Float.class) {
            return Float.class;
        } else if (valueType == XMLGregorianCalendar.class) {
            return ZonedDateTime.class;
        } else {
            return String.class;
        }
    }
}
