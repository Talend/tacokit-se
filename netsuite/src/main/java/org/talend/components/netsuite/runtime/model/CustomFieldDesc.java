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
import org.talend.components.netsuite.runtime.model.customfield.CustomFieldRefType;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Descriptor of custom field which is not declared as {@code member field} in data object type class
 * and stored in {@code customFieldList}.
 */

@Data
@EqualsAndHashCode(callSuper = false)
@ToString
public class CustomFieldDesc extends FieldDesc {

    /** Customization ref for this field. */
    private NsRef customizationRef;

    /** Type of this custom field. */
    private CustomFieldRefType customFieldType;

    /**
     * Indicates whether this field is applied for search or not.
     */
    private boolean globalSearch;
}
