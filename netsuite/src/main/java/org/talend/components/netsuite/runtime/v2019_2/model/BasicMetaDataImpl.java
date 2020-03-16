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
package org.talend.components.netsuite.runtime.v2019_2.model;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.talend.components.netsuite.runtime.client.NetSuiteException;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.runtime.model.RecordTypeDesc;
import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.runtime.model.beans.Beans;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorType;
import org.talend.components.netsuite.runtime.model.search.SearchFieldOperatorTypeDesc;

import com.netsuite.webservices.v2019_2.platform.core.BaseRef;
import com.netsuite.webservices.v2019_2.platform.core.CustomFieldList;
import com.netsuite.webservices.v2019_2.platform.core.CustomFieldRef;
import com.netsuite.webservices.v2019_2.platform.core.ListOrRecordRef;
import com.netsuite.webservices.v2019_2.platform.core.NullField;
import com.netsuite.webservices.v2019_2.platform.core.SearchBooleanCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchBooleanField;
import com.netsuite.webservices.v2019_2.platform.core.SearchCustomFieldList;
import com.netsuite.webservices.v2019_2.platform.core.SearchDateCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchDateField;
import com.netsuite.webservices.v2019_2.platform.core.SearchDoubleCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchDoubleField;
import com.netsuite.webservices.v2019_2.platform.core.SearchEnumMultiSelectCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchEnumMultiSelectField;
import com.netsuite.webservices.v2019_2.platform.core.SearchLongCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchLongField;
import com.netsuite.webservices.v2019_2.platform.core.SearchMultiSelectCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchMultiSelectField;
import com.netsuite.webservices.v2019_2.platform.core.SearchStringCustomField;
import com.netsuite.webservices.v2019_2.platform.core.SearchStringField;
import com.netsuite.webservices.v2019_2.platform.core.SearchTextNumberField;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchDate;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchDateFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchDoubleFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchEnumMultiSelectFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchLongFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchMultiSelectFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchStringFieldOperator;
import com.netsuite.webservices.v2019_2.platform.core.types.SearchTextNumberFieldOperator;

public class BasicMetaDataImpl extends BasicMetaData {

    private static final LazyInitializer<BasicMetaDataImpl> initializer = new LazyInitializer<BasicMetaDataImpl>() {

        @Override
        protected BasicMetaDataImpl initialize() throws ConcurrentException {
            return new BasicMetaDataImpl();
        }
    };

    public static BasicMetaDataImpl getInstance() {
        try {
            return initializer.get();
        } catch (ConcurrentException e) {
            throw new NetSuiteException("Initialization error", e);
        }
    }

    public BasicMetaDataImpl() {
        bindTypeHierarchy(BaseRef.class);
        bindTypeHierarchy(CustomFieldRef.class);

        bindType(NullField.class, null);
        bindType(ListOrRecordRef.class, null);
        bindType(CustomFieldList.class, null);
        bindType(SearchCustomFieldList.class, null);

        bindSearchFields(Arrays.asList(SearchBooleanCustomField.class, SearchBooleanField.class, SearchDateCustomField.class,
                SearchDateField.class, SearchDoubleCustomField.class, SearchDoubleField.class, SearchEnumMultiSelectField.class,
                SearchEnumMultiSelectCustomField.class, SearchMultiSelectCustomField.class, SearchMultiSelectField.class,
                SearchLongCustomField.class, SearchLongField.class, SearchStringCustomField.class, SearchStringField.class,
                SearchTextNumberField.class));

        bindSearchFieldOperatorTypes(Arrays.asList(
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.DATE, SearchDateFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.PREDEFINED_DATE, SearchDate.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.LONG, SearchLongFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.DOUBLE, SearchDoubleFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.STRING, SearchStringFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.TEXT_NUMBER,
                        SearchTextNumberFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.MULTI_SELECT,
                        SearchMultiSelectFieldOperator.class),
                SearchFieldOperatorTypeDesc.createForEnum(SearchFieldOperatorType.ENUM_MULTI_SELECT,
                        SearchEnumMultiSelectFieldOperator.class)));
    }

    @Override
    public Collection<RecordTypeDesc> getRecordTypes() {
        return Arrays.<RecordTypeDesc> asList(RecordTypeEnum.values());
    }

    @Override
    public RecordTypeDesc getRecordType(String recordType) {
        return RecordTypeEnum.getByTypeName(Beans.toInitialUpper(recordType));
    }

    @Override
    public SearchRecordTypeDesc getSearchRecordType(String searchRecordType) {
        return SearchRecordTypeEnum.getByTypeName(Beans.toInitialUpper(searchRecordType));
    }
}