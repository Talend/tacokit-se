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

import java.util.Collection;
import java.util.List;

import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.RecordTypeInfo;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.TypeDesc;

/**
 * Provides information about NetSuite data model.
 */
public interface MetaDataSource {

    /**
     * Return meta data about basic (standard) data model, without customizations.
     *
     * @return basic meta data model
     */
    BasicMetaData getBasicMetaData();

    /**
     * Return all available record types including custom record types.
     *
     * @return list of record types
     */
    Collection<RecordTypeInfo> getRecordTypes(boolean customizationEnabled);

    /**
     * Return type descriptor for a given model object type's name
     *
     * @param typeName model object type's name
     * @return type descriptor
     */
    TypeDesc getTypeInfo(String typeName, boolean customizationEnabled);

    /**
     * Return information about a record type by it's name.
     *
     * @param typeName name of record type
     * @return record type information
     */
    RecordTypeInfo getRecordType(String typeName, boolean customizationEnabled);

    /**
     * Return search record type descriptor by a record type's name.
     *
     * @param recordTypeName name of record type
     * @return search record type descriptor
     */
    SearchRecordTypeDesc getSearchRecordType(String recordTypeName, boolean customizationEnabled);

    /**
     * Return search record type descriptor by a record type descriptor.
     *
     * @param recordType record type descriptor
     * @return search record type descriptor
     */
    SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType);

    /**
     * Return list search fields by a record type's name.
     *
     * @param recordTypeName name of record type
     * @return search record fields list
     */
    List<String> getSearchRecordCustomFields(String recordTypeName, boolean customizationEnabled);
}
