/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.salesforce.service.operation.converters;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.sobject.SObject;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SObjectRelationShip {

    private final String lookupRelationshipFieldName;

    private final String lookupFieldModuleName;

    private final String lookupFieldExternalIdName;

    public void setValue(SObject so, FieldType fieldType, Object value) {

        so.setField(lookupRelationshipFieldName, null);
        so.getChild(lookupRelationshipFieldName).setField("type", lookupFieldModuleName);

        new FieldSetter(so.getChild(lookupRelationshipFieldName))
                .addSObjectField(this.newField(lookupFieldExternalIdName, fieldType), value);
    }

    private Field newField(String name, FieldType fType) {
        final Field f = new Field();
        f.setName(name);
        f.setType(fType);
        return f;
    }
}
