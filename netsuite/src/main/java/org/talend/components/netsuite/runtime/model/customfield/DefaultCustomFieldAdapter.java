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

/**
 * Generic custom field adapter for other custom field record types which have no specialized adapter.
 */
public class DefaultCustomFieldAdapter<T> extends CustomFieldAdapter<T> {

    private boolean applies;

    public DefaultCustomFieldAdapter(BasicRecordType type, boolean applies) {
        super(type);

        this.applies = applies;
    }

    @Override
    public boolean appliesTo(String recordTypeName, T field) {
        return applies;
    }

    @Override
    public CustomFieldRefType apply(T field) {
        return applies ? getFieldType(field) : null;
    }

    @Override
    protected String getPropertyName(String recordTypeName) {
        return null;
    }

}
