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
package org.talend.components.netsuite.runtime.model.customfield;

import org.talend.components.netsuite.runtime.model.BasicRecordType;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom field adapter for {@link BasicRecordType#CRM_CUSTOM_FIELD} type.
 */
public class CrmCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private static final Map<String, String> CRM_TYPE_PROPERTY_MAP = new HashMap<>();
    static {
        CRM_TYPE_PROPERTY_MAP.put("campaign", "appliesToCampaign");
        CRM_TYPE_PROPERTY_MAP.put("supportCase", "appliesToCase");
        CRM_TYPE_PROPERTY_MAP.put("calendarEvent", "appliesToEvent");
        CRM_TYPE_PROPERTY_MAP.put("issue", "appliesToIssue");
        CRM_TYPE_PROPERTY_MAP.put("phoneCall", "appliesToPhoneCall");
        CRM_TYPE_PROPERTY_MAP.put("projectTask", "appliesToProjectTask");
        CRM_TYPE_PROPERTY_MAP.put("solution", "appliesToSolution");
        CRM_TYPE_PROPERTY_MAP.put("task", "appliesToTask");
    }

    public CrmCustomFieldAdapter() {
        super(BasicRecordType.CRM_CUSTOM_FIELD);
    }

    @Override
    public String getPropertyName(String recordType) {
        return CRM_TYPE_PROPERTY_MAP.get(recordType);
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return getFieldType(field);
    }
}
