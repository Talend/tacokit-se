package org.talend.components.salesforce.service.operation.converters;

import java.math.BigDecimal;
import java.util.Date;

import com.sforce.soap.partner.Field;
import com.sforce.soap.partner.FieldType;
import com.sforce.ws.bind.CalendarCodec;
import com.sforce.ws.bind.DateCodec;
import com.sforce.ws.bind.XmlObject;
import com.sforce.ws.types.Time;
import com.sforce.ws.util.Base64;

import org.apache.avro.util.Utf8;
import org.talend.components.salesforce.commons.SalesforceRuntimeHelper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class FieldSetter {

    private DateCodec dateCodec = new DateCodec();

    private CalendarCodec calendarCodec = new CalendarCodec();

    private final XmlObject xmlObject;

    public void addSObjectField(Field field, Object value) {
        if (value == null || value.toString().isEmpty()) {
            return;
        }
        Object valueToAdd = value;
        if (Utf8.class.isInstance(value)) {
            valueToAdd = value.toString();
        }
        // Convert stuff here
        // For Nillable base64 type field, we retrieve it as UNION type:[bytes,null]
        // So need to unwrap it and get its real type
        if (FieldType.base64.equals(field.getType())) {
            if ((value instanceof String) || (value instanceof byte[])) {
                byte[] base64Data = null;
                if (value instanceof byte[]) {
                    base64Data = (byte[]) value;
                } else {
                    base64Data = ((String) value).getBytes();
                }
                if (Base64.isBase64(new String(base64Data))) {
                    valueToAdd = Base64.decode(base64Data);
                }
            }
        }
        String fieldName = field.getName();
        if (fieldName != null && valueToAdd instanceof String) {
            switch (field.getType()) {
            case _boolean:
                xmlObject.setField(fieldName, Boolean.valueOf((String) valueToAdd));
                break;
            case _double:
            case percent:
                xmlObject.setField(fieldName, Double.valueOf((String) valueToAdd));
                break;
            case _int:
                xmlObject.setField(fieldName, Integer.valueOf((String) valueToAdd));
                break;
            case currency:
                xmlObject.setField(fieldName, new BigDecimal((String) valueToAdd));
                break;
            case date:
                xmlObject.setField(fieldName, dateCodec.deserialize((String) valueToAdd));
                break;
            case datetime:
                xmlObject.setField(fieldName, calendarCodec.deserialize((String) valueToAdd));
                break;
            case time:
                xmlObject.setField(fieldName, new Time((String) valueToAdd));
                break;
            case base64:
            default:
                xmlObject.setField(fieldName, valueToAdd);
                break;
            }
        } else {
            if (valueToAdd instanceof Date) {
                xmlObject.setField(fieldName, SalesforceRuntimeHelper.convertDateToCalendar((Date) valueToAdd, true));
            } else {
                xmlObject.setField(fieldName, valueToAdd);
            }
        }
    }
}
