package org.talend.components.salesforce.service.operation.converters;

import java.util.GregorianCalendar;
import java.util.Map;
import java.util.function.Supplier;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.soap.partner.sobject.SObject;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.RequiredArgsConstructor;


@RequiredArgsConstructor
public class SObjectConvertorForUpdate {

    private final Supplier<Map<String, Field>> fieldMap;

    private final Map<String, SObjectRelationShip> referenceFieldsMap;

    private final String moduleName;

    private final String upsertKeyColumn;

    public SObject fromRecord(Record input) {
        SObject so = new SObject();
        so.setType(moduleName);

        final FieldSetter setter = new FieldSetter(so);
        for (Schema.Entry field : input.getSchema().getEntries()) {
            Object value = null;
            if (Schema.Type.DATETIME.equals(field.getType())) {
                value = GregorianCalendar.from(input.getDateTime(field.getName()));
            } else {
                value = input.get(Object.class, field.getName());
            }
            Field sfField = fieldMap.get().get(field.getName());
            /*
             * if (sfField == null) {
             * continue;
             * }
             */
            if (value != null && !"".equals(value.toString())) {
                if (referenceFieldsMap != null && referenceFieldsMap.get(field.getName()) != null) {
                    final SObjectRelationShip relationMap = referenceFieldsMap.get(field.getName());
                    if (relationMap != null) {
                        relationMap.setValue(so, sfField.getType(), value);
                    }
                } else {
                    // Skip column "Id" for upsert, when "Id" is not specified as "upsertKey.Column"
                    if (!"Id".equals(field.getName()) || field.getName().equals(upsertKeyColumn)) {
                        if (sfField != null) {
                            // The real type is need in addSObjectField()
                            setter.addSObjectField(sfField, value);
                        } else {
                            // This is keep old behavior, when set a field which is not exist.
                            // It would throw a exception for this.
                            setter.addSObjectField(this.newField(field.getName(), FieldType.string), value);
                        }
                    }
                }
            } /*else {
                if (referenceFieldsMap != null && referenceFieldsMap.get(field.getName()) != null) {
                    Map<String, String> relationMap = referenceFieldsMap.get(field.getName());
                    String lookupFieldName = relationMap.get("lookupFieldName");

                }
            }*/
        }
        // TODO ignoreNull
       /* if (false) {
            so.setFieldsToNull(nullValueFields.toArray(new String[0]));
        }*/
        return so;
    }

    private Field newField(String name, FieldType fType) {
        final Field f = new Field();
        f.setName(name);
        f.setType(fType);
        return f;
    }

}
