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
package org.talend.components.netsuite.runtime.model.beans;

/**
 * Used to access enum values for enum classes generated from NetSuite's XML schemas.
 */
public interface EnumAccessor<T> {

    /**
     * Get string value for given enum constant.
     *
     * @param enumValue enum value
     * @return string value
     */
    String getStringValue(T enumValue);

    /**
     * Get enum constant for given string value.
     *
     * @param value string value
     * @return enum constant
     */
    T getEnumValue(String value);
}
