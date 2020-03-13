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

import org.talend.components.netsuite.runtime.model.CustomFieldDesc;
import org.talend.components.netsuite.runtime.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;

import java.util.Collection;
import java.util.Map;

/**
 * Provides meta information about customizations of NetSuite domain model.
 */
public interface CustomMetaDataSource {

    /**
     * Return custom record types.
     *
     * @return set of custom record types
     */
    Collection<CustomRecordTypeInfo> getCustomRecordTypes();

    /**
     * Return information about a record type.
     *
     * @param typeName name of record type
     * @return record type info or <code>null</code> if record type was not found
     */
    CustomRecordTypeInfo getCustomRecordType(String typeName);

    /**
     * Return custom fields for a record type.
     *
     * @param recordTypeInfo record type which to return custom fields for
     * @return custom field map which contains <code>(custom field name, custom field descriptor)</code> entries
     */
    Map<String, CustomFieldDesc> getCustomFields(RecordTypeInfo recordTypeInfo);
}
