package org.talend.components.salesforce.service.operation.converters;

import java.util.GregorianCalendar;
import java.util.Map;
import java.util.function.Supplier;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.sobject.SObject;

import org.talend.components.salesforce.configuration.OutputConfig;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SObjectConverter {

    private final Supplier<Map<String, Field>> fieldMap;

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
                final Field sfField = fieldMap.get().get(field.getName());
                if (sfField == null) {
                    continue;
                }
                setter.addSObjectField(sfField, value);
            }
        }

        return so;
    }



}
