/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */
package org.talend.components.salesforce.commons;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.talend.components.salesforce.commons.converter.AsStringConverter;
import org.talend.components.salesforce.commons.converter.BooleanConverter;
import org.talend.components.salesforce.commons.converter.ByteConverter;
import org.talend.components.salesforce.commons.converter.BytesConverter;
import org.talend.components.salesforce.commons.converter.DateConverter;
import org.talend.components.salesforce.commons.converter.DecimalConverter;
import org.talend.components.salesforce.commons.converter.DoubleConverter;
import org.talend.components.salesforce.commons.converter.FloatConverter;
import org.talend.components.salesforce.commons.converter.IntegerConverter;
import org.talend.components.salesforce.commons.converter.LongConverter;
import org.talend.components.salesforce.commons.converter.ShortConverter;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.avro.converter.AvroConverter;

import com.sforce.soap.partner.DescribeSObjectResult;
import com.sforce.soap.partner.Field;

/**
 *
 */
public class SalesforceAvroRegistry {

    private static final SalesforceAvroRegistry sInstance = new SalesforceAvroRegistry();

    public static SalesforceAvroRegistry get() {
        return sInstance;
    }

    public Schema inferSchema(DescribeSObjectResult in) {
        if (in == null) {
            return null;
        }
        List<Schema.Field> fields = new ArrayList<>();
        for (Field field : in.getFields()) {

            Schema.Field avroField = new Schema.Field(field.getName(), inferSchema(field), null, field.getDefaultValueFormula());
            // Add some Talend6 custom properties to the schema.
            Schema avroFieldSchema = avroField.schema();
            if (avroFieldSchema.getType() == Schema.Type.UNION) {
                for (Schema schema : avroFieldSchema.getTypes()) {
                    if (avroFieldSchema.getType() != Schema.Type.NULL) {
                        avroFieldSchema = schema;
                        break;
                    }
                }
            }
            if (AvroUtils.isSameType(avroFieldSchema, AvroUtils._string())) {
                if (field.getLength() != 0) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_LENGTH, String.valueOf(field.getLength()));
                }
                if (field.getPrecision() != 0) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_PRECISION, String.valueOf(field.getPrecision()));
                }
            } else {
                if (field.getPrecision() != 0) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_LENGTH, String.valueOf(field.getPrecision()));
                }
                if (field.getScale() != 0) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_PRECISION, String.valueOf(field.getScale()));
                }
            }

            // pattern will be removed when we have db type for salesforce
            switch (field.getType()) {
            case date:
                avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd");
                break;
            case datetime:
                avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'");
                break;
            case time:
                avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "HH:mm:ss.SSS'Z'");
                break;
            default:
                break;
            }
            if (field.getDefaultValue() != null) {
                // FIXME really needed as Schema.Field has ability to store default value
                avroField.addProp(SchemaConstants.TALEND_COLUMN_DEFAULT, String.valueOf(field.getDefaultValue()));
            }
            fields.add(avroField);
        }
        return Schema.createRecord(in.getName(), null, null, false, fields);
    }

    /**
     * Infers an Avro schema for the given Salesforce Field. This can be an expensive operation so the schema should be
     * cached where possible. The return type will be the Avro Schema that can contain the field data without loss of
     * precision.
     *
     * @param field the Field to analyse.
     * @return the schema for data that the field describes.
     */
    private Schema inferSchema(Field field) {
        // Logic taken from:
        // https://github.com/Talend/components/blob/aef0513e0ba6f53262b89ef2ea8a981cd1430d47/components-salesforce/src/main/java/org/talend/components/salesforce/runtime/SalesforceSourceOrSink.java#L214

        // Field type information at:
        // https://developer.salesforce.com/docs/atlas.en-us.200.0.object_reference.meta/object_reference/primitive_data_types.htm

        // Note: default values are at the field level, not attached to the field.
        // However, these properties are saved in the schema with Talend6SchemaConstants if present.
        Schema base;
        switch (field.getType()) {
        case _boolean:
            base = AvroUtils._boolean();
            break;
        case _double:
            base = AvroUtils._double();
            break;
        case percent:
            base = AvroUtils._double();
            break;
        case _int:
            base = AvroUtils._int();
            break;
        case currency:
            base = AvroUtils._decimal();
            break;
        case date:
        case datetime:
            base = AvroUtils._logicalTimestamp();
            break;
        case base64:
            base = AvroUtils._bytes();
            break;
        default:
            base = AvroUtils._string();
            break;
        }
        base = field.getNillable() ? AvroUtils.wrapAsNullable(base) : base;

        return base;
    }

    /**
     * A helper method to convert the String representation of a datum in the Salesforce system to the Avro type that
     * matches the Schema generated for it.
     *
     * @param f
     * @return
     */
    public AvroConverter<String, ?> getConverterFromString(Schema.Field f) {
        Schema fieldSchema = AvroUtils.unwrapIfNullable(f.schema());
        LogicalType logicalType = fieldSchema.getLogicalType();
        if(LogicalTypes.timestampMillis().equals(logicalType)){
            return new DateConverter(f);
        }else {
            if (AvroUtils.isSameType(fieldSchema, AvroUtils._boolean())) {
                return new BooleanConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._decimal())) {
                return new DecimalConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._double())) {
                return new DoubleConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._float())) {
                return new FloatConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._int())) {
                return new IntegerConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._byte())) {
                return new ByteConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._short())) {
                return new ShortConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._long())) {
                return new LongConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._bytes())) {
                return new BytesConverter(f);
            } else if (AvroUtils.isSameType(fieldSchema, AvroUtils._string())) {
                return new AsStringConverter<Object>(f) {

                    @Override public Object convertToAvro(String s) {
                        return s;
                    }
                };
            }
            throw new UnsupportedOperationException("The type " + fieldSchema.getType() + " is not supported."); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

}
