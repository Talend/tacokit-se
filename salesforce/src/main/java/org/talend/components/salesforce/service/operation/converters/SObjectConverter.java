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

import java.util.GregorianCalendar;
import java.util.Map;
import java.util.function.Supplier;

import com.sforce.soap.partner.IField;
import com.sforce.soap.partner.sobject.SObject;

import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SObjectConverter {

    private final Supplier<Map<String, IField>> fieldMap;

    private final String moduleName;

    public SObject fromRecord(Record input, OutputConfig.OutputAction outputAction) {
        SObject so = new SObject();
        so.setType(moduleName);

        final FieldSetter setter = new FieldSetter(so);
        for (Schema.Entry field : input.getSchema().getEntries()) {
            // For "Id" column, we should ignore it for "INSERT" action
            if (!("Id".equals(field.getName()) && OutputConfig.OutputAction.INSERT.equals(outputAction))) {
                Object value = null;
                if (Schema.Type.DATETIME.equals(field.getType())) {
                    value = GregorianCalendar.from(input.getDateTime(field.getName()));
                } else {
                    value = input.get(Object.class, field.getName());
                }
                // TODO need check
                final IField sfField = fieldMap.get().get(field.getName());
                if (sfField == null) {
                    continue;
                }
                setter.addSObjectField(sfField, value);
            }
        }

        return so;
    }

}
