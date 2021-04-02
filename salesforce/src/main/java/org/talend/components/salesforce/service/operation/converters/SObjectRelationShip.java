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
