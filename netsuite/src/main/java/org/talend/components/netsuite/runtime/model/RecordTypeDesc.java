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

/**
 * Descriptor of NetSuite record type.
 *
 * <p>
 * Implementation is provided by concrete version of NetSuite runtime.
 *
 * @see SearchRecordTypeDesc
 */
public interface RecordTypeDesc {

    /**
     * Name of record type.
     *
     * @return type
     */
    String getType();

    /**
     * Get short name of record data object type.
     *
     * @return short name of record data object type
     */
    String getTypeName();

    /**
     * Get class of record data object type.
     *
     * @return class
     */
    Class<?> getRecordClass();

    /**
     * Get name of search record type corresponding to this record type.
     *
     * @see SearchRecordTypeDesc#getType()
     *
     * @return name of search record type
     */
    String getSearchRecordType();
}
