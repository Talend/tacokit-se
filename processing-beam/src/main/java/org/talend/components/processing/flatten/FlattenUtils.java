package org.talend.components.processing.flatten;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.exception.TalendRuntimeException;
import org.talend.daikon.exception.error.CommonErrorCodes;

import java.util.ArrayList;
import java.util.List;

public class FlattenUtils {

    public static Schema getUnwrappedSchema(Schema.Field field) {
        return AvroUtils.unwrapIfNullable(field.schema());
    }

    /**
     * Get the sub-fields from inputRecord of the field columnName.
     *
     * @return list contains the sub-fields from inputRecord of the field columnName
     */
    public static List<Object> getInputFields(IndexedRecord inputRecord, String columnName) {
        ArrayList<Object> inputFields = new ArrayList<Object>();
        String[] path = columnName.split("\\.");
        Schema schema = inputRecord.getSchema();

        for (Integer i = 0; i < path.length; i++) {
            // The column was existing on the input record, we forward it to the output record.
            if (schema.getField(path[i]) == null) {
                throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_ARGUMENT,
                        new Throwable(String.format("The field %s is not present on the input record", columnName)));
            }
            Object inputValue = inputRecord.get(schema.getField(path[i]).pos());

            // The current column can be a Record (an hierarchical sub-object)
            // or directly a value.
            if (inputValue instanceof GenericData.Record) {
                // If we are on a record, we need to recursively do the process
                inputRecord = (IndexedRecord) inputValue;

                // The sub-schema at this level is a union of "empty" and a
                // record, so we need to get the true sub-schema
                if (schema.getField(path[i]).schema().getType().equals(Schema.Type.RECORD)) {
                    schema = schema.getField(path[i]).schema();
                    if (i == path.length - 1) {
                        inputFields.add(inputValue);
                    }
                } else if (schema.getField(path[i]).schema().getType().equals(Schema.Type.UNION)) {
                    if (i == path.length - 1) {
                        inputFields.add(inputValue);
                        break;
                    }
                    for (Schema childSchema : schema.getField(path[i]).schema().getTypes()) {
                        if (childSchema.getType().equals(Schema.Type.RECORD)) {
                            schema = childSchema;
                            break;
                        }
                    }
                }
            } else if (inputValue instanceof List) {
                for (int j = 0; j < ((List) inputValue).size(); j++) {
                    inputFields.add(((List) inputValue).get(j));
                }
                break;
            } else {
                if (i == path.length - 1) {
                    inputFields.add(inputValue);
                }
            }
        }

        return inputFields;
    }

    /**
     * Transform input schema to a new schema.
     *
     * The schema of the array field `pathToNormalize` will be modified to the schema of its fields.
     */
    public static Schema transformSchema(Schema inputSchema, String[] pathToNormalize, int pathIterator) {
        List<Schema.Field> fieldList = new ArrayList<>();
        for (Schema.Field field : inputSchema.getFields()) {
            Schema unwrappedSchema = getUnwrappedSchema(field);
            if ((pathIterator < pathToNormalize.length) && (field.name().equals(pathToNormalize[pathIterator]))
                    && (unwrappedSchema.getType().equals(Schema.Type.ARRAY))) {
                fieldList.add(new Schema.Field(field.name(), unwrappedSchema.getElementType(), field.doc(), field.defaultVal()));
            } else if (unwrappedSchema.getType().equals(Schema.Type.RECORD)) {
                if ((pathIterator < pathToNormalize.length) && (field.name().equals(pathToNormalize[pathIterator]))) {
                    Schema subElementSchema = transformSchema(unwrappedSchema, pathToNormalize, ++pathIterator);
                    fieldList.add(new Schema.Field(field.name(), subElementSchema, null, null));
                } else {
                    // if we are outside of the pathToNormalize, set the pathIterator at something that cannot be used
                    // again
                    Schema subElementSchema = transformSchema(unwrappedSchema, pathToNormalize, pathToNormalize.length);
                    fieldList.add(new Schema.Field(field.name(), subElementSchema, null, null));
                }
            } else {
                // element add it directly
                fieldList.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
            }
        }
        return Schema.createRecord(inputSchema.getName(), inputSchema.getDoc(), inputSchema.getNamespace(), inputSchema.isError(),
                fieldList);

    }

    /**
     * Get the child schema of a field, and check if this is correctly a Record. If not, thow a TalendRuntimeException.
     *
     * @param parentSchema the schema of the parent element
     * @param field the field to extract
     * @return the schema of the element extracted
     */
    public static Schema getChildSchemaAsRecord(Schema parentSchema, Schema.Field field) {
        Schema childSchema = AvroUtils.unwrapIfNullable(parentSchema.getField(field.name()).schema());
        if (childSchema.getType().equals(Schema.Type.RECORD)) {
            return childSchema;
        } else {
            throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_EXCEPTION,
                    new Throwable(String.format("The field %s has the type %s but should be a Record on the schema %s",
                            field.name(), childSchema.getType(), parentSchema.toString())));
        }
    }

    /**
     * Get the child schema of a field, and check if this is correctly a Record. If not, thow a TalendRuntimeException.
     *
     * @param parentSchema the schema of the parent element
     * @param field the field to extract
     * @return the schema of the element extracted
     */
    public static Schema getChildSchemaOfListAsRecord(Schema parentSchema, Schema.Field field) {
        Schema childSchema = AvroUtils.unwrapIfNullable(parentSchema.getField(field.name()).schema()).getElementType();
        if (childSchema.getType().equals(Schema.Type.RECORD)) {
            return childSchema;
        } else {
            throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_EXCEPTION,
                    new Throwable(String.format("The field %s has the type %s but should be a Record on the schema %s",
                            field.name(), childSchema.getType(), parentSchema.toString())));
        }
    }

    /**
     * Generate a new Record which contains the normalized value `outputValue`.
     */
    public static GenericRecord generateNormalizedRecord(IndexedRecord inputRecord, Schema inputSchema, Schema outputSchema,
            String[] pathToNormalize, int pathIterator, Object outputValue) {
        GenericRecordBuilder outputRecord = new GenericRecordBuilder(outputSchema);

        for (Schema.Field field : inputSchema.getFields()) {
            if (outputSchema.getField(field.name()) != null) {
                // The column was existing on the input record, we forward it to the output record.
                Object inputValue = inputRecord.get(inputSchema.getField(field.name()).pos());

                if ((pathToNormalize.length > 0) && (pathIterator < pathToNormalize.length)
                        && (field.name().equals(pathToNormalize[pathIterator]))) {
                    // The current column can be a Record (an hierarchical sub-object), a list or directly a value.
                    // If we are on a record, we need to reach the element to normalize
                    // if we are on a object or a list (aka the element to normalize), we save it to the output.
                    if (inputValue instanceof GenericData.Record) {
                        // use recursively to reach the element to normalize
                        Schema inputChildSchema = getChildSchemaAsRecord(inputSchema, field);
                        Schema outputChildSchema = getChildSchemaAsRecord(outputSchema, field);
                        Object childRecord = generateNormalizedRecord((IndexedRecord) inputValue, inputChildSchema,
                                outputChildSchema, pathToNormalize, ++pathIterator, outputValue);
                        outputRecord.set(field.name(), childRecord);
                    } else if (inputValue instanceof List) {
                        // a list if we are on element to normalize.
                        // If we reach a list and we are not on the final element, this is a case that is currently
                        // not available.
                        if (pathIterator == pathToNormalize.length - 1) {
                            outputRecord.set(field.name(), outputValue);
                        } else {
                            throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_ARGUMENT,
                                    new Throwable(String.format("You cannot reach element to normalize inside a list.")));
                        }
                    } else {
                        // Reach the element to normalize => duplicate it.
                        if (pathIterator == pathToNormalize.length - 1) {
                            outputRecord.set(field.name(), outputValue);
                        } else {
                            // We should never use the method normalize to access leaf element.
                            // it should be done on duplicateRecord.
                            throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_ARGUMENT,
                                    new Throwable(String.format(
                                            "Accessing to leaf element of the field %s should be done on duplicate record",
                                            field.name())));
                        }
                    }
                } else {
                    // We are on an element that is not on the path to the element to normalize.
                    // We just have to duplicate everything.
                    if (inputValue instanceof GenericData.Record) {
                        // Use a specific function to duplicate elements.
                        Schema inputChildSchema = getChildSchemaAsRecord(inputSchema, field);
                        Schema outputChildSchema = getChildSchemaAsRecord(outputSchema, field);
                        Object childRecord = duplicateRecord((IndexedRecord) inputValue, inputChildSchema, outputChildSchema);
                        outputRecord.set(field.name(), childRecord);
                    } else {
                        outputRecord.set(field.name(), inputValue);
                    }
                }
            } else {
                throw new TalendRuntimeException(CommonErrorCodes.UNEXPECTED_ARGUMENT,
                        new Throwable(String.format("The field %s is not present on the input schema", field.name())));
            }
        }
        return outputRecord.build();
    }

    /**
     * Generate a new Index Record which is the duplicated of the input record.
     *
     * @return the new record
     */
    public static GenericRecord duplicateRecord(IndexedRecord inputRecord, Schema inputSchema, Schema outputSchema) {
        GenericRecordBuilder outputRecord = new GenericRecordBuilder(outputSchema);
        for (Schema.Field field : inputSchema.getFields()) {
            // The column was existing on the input record, we forward it to the output record.
            Object inputValue = inputRecord.get(inputSchema.getField(field.name()).pos());

            // The current column can be a Record (an hierarchical sub-object), a list or directly a value.
            // If we are on a record, we need to recursively duplicate the element
            // If we are on a list, we need to duplicate each element of the list
            // if we are on a object, we duplicate it to the output.
            if (inputValue instanceof GenericData.Record) {
                // The sub-schema at this level is a union of "empty" and a record,
                // so we need to get the true sub-schema
                Schema inputChildSchema = getChildSchemaAsRecord(inputSchema, field);
                Schema outputChildSchema = getChildSchemaAsRecord(outputSchema, field);
                Object childRecord = duplicateRecord((IndexedRecord) inputValue, inputChildSchema, outputChildSchema);
                outputRecord.set(field.name(), childRecord);
            } else if (inputValue instanceof List) {
                // We are on a list, duplicate each sub element
                List<Object> outputElements = new ArrayList<>();
                for (Object element : (List<Object>) inputValue) {
                    Schema inputChildSchema = getChildSchemaOfListAsRecord(inputSchema, field);
                    Schema outputChildSchema = getChildSchemaOfListAsRecord(outputSchema, field);
                    outputElements.add(duplicateRecord((IndexedRecord) element, inputChildSchema, outputChildSchema));
                }
                outputRecord.set(field.name(), outputElements);
            } else {
                // We are on a raw type, use it directly
                outputRecord.set(field.name(), inputValue);
            }
        }
        return outputRecord.build();
    }

    /**
     * Check if the list contains a simple field.
     * 
     * @return true if list parameter contains a simple field
     */
    public static boolean isSimpleField(List<Object> list) {
        if (list != null && list.size() == 1 && !(list.get(0) instanceof GenericRecord)) {
            return true;
        }
        return false;
    }

    /**
     * Splits toSplit parameter around matches of the given delim parameter.
     *
     * @param toSplit string to split
     * @param delim the delimiting regular expression
     * @param isDiscardTrailingEmptyStr
     * @param isTrim
     * @return substrings
     */
    public static List<Object> delimit(String toSplit, String delim, boolean isDiscardTrailingEmptyStr, boolean isTrim) {

        String[] strSplitted = toSplit.split(delim);
        List<Object> strList = new ArrayList<Object>();
        for (int i = 0; i < strSplitted.length; i++) {
            if (isDiscardTrailingEmptyStr) {
                strSplitted[i] = strSplitted[i].replaceAll("\\s+$", "");
            }
            if (isTrim) {
                strSplitted[i] = strSplitted[i].trim();
            }
            strList.add(strSplitted[i]);
        }
        return strList;
    }
}
