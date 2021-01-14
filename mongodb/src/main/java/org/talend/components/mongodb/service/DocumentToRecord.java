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
package org.talend.components.mongodb.service;

import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.Code;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Translate mongodb document object to record.
 *
 * CAUTIONS :
 * records don't support variables objects in array ..
 * while in json/document, array can contains different kind of type :
 * `[ "Is a String", 123, { "field": "value" }, [1, 2, "text"] ]`
 * records does not.
 */
@Slf4j
public class DocumentToRecord {

    /** record facotry */
    private final RecordBuilderFactory recordBuilderFactory;

    public DocumentToRecord(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    /**
     * Guess schema from document object.
     *
     * @param document : document object.
     * @return guess record schema.
     */
    public Schema inferSchema(final Document document) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        populateDocumentEntries(builder, document);
        return builder.build();
    }

    /**
     * Convert document object to record (with guessing schema).
     *
     * @param document : document data.
     * @return data in record format.
     */
    public Record toRecord(final Document document) {
        if (document == null) {
            return null;
        }

        final Schema schema = inferSchema(document);
        return convertDocumentToRecord(schema, document);
    }

    private Schema inferSchema(final List array, DatatypeHolder data_type_holder) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.ARRAY);
        final Schema subSchema;

        if (array.isEmpty()) {
            // use String type for the miss element of empty array
            subSchema = recordBuilderFactory.newSchemaBuilder(Type.STRING).build();
            builder.withElementSchema(subSchema);
            return builder.build();
        }

        final Object value = array.get(0);

        if (isNull(value)) {
            // use String type for null value which no way to detect type
            subSchema = recordBuilderFactory.newSchemaBuilder(Type.STRING).build();
        } else if (isDocument(value)) {
            // if first element is object, supposing all elements are object (otherwise not compatible with record),
            // merge all object schemas.
            final Document document = mergeAll(array);
            subSchema = inferSchema(document);
        } else if (isArray(value)) {
            subSchema = inferSchema((List) value, data_type_holder);
        } else {
            final Type type = translateType(value, data_type_holder);
            subSchema = recordBuilderFactory.newSchemaBuilder(type).build();
        }
        builder.withElementSchema(subSchema);
        return builder.build();
    }

    private boolean isDocument(Object value) {
        return value instanceof Document;
    }

    private boolean isArray(Object value) {
        return value instanceof List;
    }

    private boolean isNull(Object value) {
        return value == null;
    }

    /**
     * create a merged object for records.
     * allow array of differents document.
     * [ { "f1": "v1"}, {"f1":"v11", "f2": "V2"} ]
     */
    private Document mergeAll(List array) {
        final Document document = new Document();

        array.stream().filter((Object v) -> v instanceof Document).forEach(doc -> {
            document.putAll((Map<String, Object>) doc);
        });

        return document;
    }

    private void populateDocumentEntries(Schema.Builder builder, Document value) {
        value.entrySet().stream().map(s -> createEntry(s.getKey(), s.getValue())).forEach(builder::withEntry);
    }

    static final String TYPE_SPLIT_CHARS = ":$";

    private Entry createEntry(String name, Object value) {
        if (log.isDebugEnabled()) {
            log.debug("[createEntry#{}] ({}) {} ", name, value == null ? null : value.getClass(), value);
        }
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        // use comment to store the real element name, for example, "$oid"
        builder.withName(name).withComment(name);

        if (!isDocument(value)) {
            builder.withNullable(true);
        }

        DatatypeHolder data_type_holder = new DatatypeHolder();

        if (isNull(value)) {
            // use String type for null value which no way to detect type
            builder.withType(Type.STRING);
        } else if (isArray(value)) {
            final Schema subSchema = this.inferSchema((List) value, data_type_holder);
            if (subSchema != null) {
                builder.withElementSchema(subSchema).withType(Type.ARRAY);
                if (data_type_holder.data_type != null) {
                    builder.withComment(name + TYPE_SPLIT_CHARS + data_type_holder.data_type.origin_type);
                }
            }
        } else if (isDocument(value)) {
            builder.withType(Type.RECORD);
            Schema.Builder nestedSchemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            populateDocumentEntries(nestedSchemaBuilder, (Document) value);
            builder.withElementSchema(nestedSchemaBuilder.build());
        } else {
            Type type = translateType(value, data_type_holder);
            builder.withType(type);
            if (data_type_holder.data_type != null) {
                // now use comment to store the origin name and origin data type, TODO should move it to framework
                builder.withComment(name + TYPE_SPLIT_CHARS + data_type_holder.data_type.origin_type);
            }
        }

        Entry entry = builder.build();
        if (log.isDebugEnabled()) {
            log.debug("[createEntry#{}] generated ({})", name, entry);
        }
        return entry;
    }

    private Record convertDocumentToRecord(Schema schema, Document document) {
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder(schema);
        schema.getEntries().stream().forEach((Entry entry) -> this.integrateEntryToRecord(entry, builder, document));
        return builder.build();
    }

    private String getElementName(Entry entry) {
        // not use entry.getName() here as "$oid" will be correct to "oid"
        // comment store "$oid", so use comment here
        return getOriginName(entry);
    }

    private String getOriginName(Entry entry) {
        // now use comment to store origin name and origin type information, not good, TODO move to framework
        final String comment = entry.getComment();

        if (comment != null && comment.contains(DocumentToRecord.TYPE_SPLIT_CHARS)) {
            String origin_name = comment.substring(0, comment.lastIndexOf(DocumentToRecord.TYPE_SPLIT_CHARS));
            return origin_name;
        }

        return comment;
    }

    private void integrateEntryToRecord(Entry entry, Record.Builder builder, Document document) {
        if (!document.containsKey(getElementName(entry))) {
            return;
        }
        switch (entry.getType()) {
        case RECORD: {
            final Document subDocument = document.get(getElementName(entry), Document.class);
            final Record record = convertDocumentToRecord(entry.getElementSchema(), subDocument);
            builder.withRecord(entry, record);
            break;
        }
        case ARRAY:
            final List<?> objects = convertArray(entry.getElementSchema(), (List) document.get(getElementName(entry)));
            if (objects != null) {
                builder.withArray(entry, objects);
            }
            break;
        case STRING: {
            // TODO check if is right here as this is also do process for null as all null value is mapped to String type, as
            // value may be null here
            Object value = document.get(getElementName(entry));
            if (isNull(value)) {
                builder.withString(entry, (String) value);
            } else if (value instanceof ObjectId) {
                builder.withString(entry, ObjectId.class.cast(value).toString());
            } else if (value instanceof Code) {
                builder.withString(entry, Code.class.cast(value).getCode());
            } else {
                builder.withString(entry, value.toString());
            }

            break;
        }
        case INT: {
            Integer value = document.getInteger(getElementName(entry));
            builder.withInt(entry, value);
            break;
        }
        case LONG: {
            Long value = document.getLong(getElementName(entry));
            builder.withLong(entry, value);
            break;
        }
        case FLOAT: {
            // Mongo DB document don't have float type, so all double type, TODO check
            Double value = document.getDouble(getElementName(entry));
            builder.withDouble(entry, value);
            break;
        }
        case DOUBLE: {
            Double value = document.getDouble(getElementName(entry));
            builder.withDouble(entry, value);
            break;
        }
        case BOOLEAN: {
            Boolean value = document.getBoolean(getElementName(entry));
            builder.withBoolean(entry, value);
            break;
        }
        case BYTES: {
            String value = document.getString(getElementName(entry));
            // TODO use default encoding? not UTF8
            builder.withBytes(entry, value.getBytes());
            break;
        }
        case DATETIME: {
            Date value = document.getDate(getElementName(entry));
            builder.withDateTime(entry, value);
            break;
        }
        }
    }

    /**
     * Extract list of record format element from array.
     *
     * @param schema : schema of array element.
     * @param array : array.
     * @return list of value.
     */
    private List<Object> convertArray(Schema schema, List<Object> array) {
        final List<Object> result;
        Schema elementSchema = schema.getElementSchema();
        switch (elementSchema.getType()) {
        case RECORD:
            result = array.stream().map((Object v) -> convertDocumentToRecord(elementSchema, (Document) v))
                    .collect(Collectors.toList());
            break;
        case ARRAY:
            // this way can't pass the complier in an old version jdk8
            /*
             * result = array.stream().map((Object v) -> convertArray(elementSchema, (List)v))
             * .collect(Collectors.toList());
             */
            result = new ArrayList<>();
            for (Object v : array) {
                result.add(convertArray(elementSchema, (List) v));
            }
            break;
        case STRING:
            // TODO : check if right here : do process for null as all null value is mapped to String type, this is for the case :
            // {array: []} or {array: [null]}
            if (array.isEmpty()) {
                // maybe need clone?
                result = array;
            } else {
                // String.cast can process null, so ok here, not sure how this process empty array
                result = new ArrayList<>();
                array.stream().forEach(v -> {
                    if (isNull(v)) {
                        result.add((String) v);
                    } else if (v instanceof ObjectId) {
                        result.add(ObjectId.class.cast(v).toString());
                    } else if (v instanceof Code) {
                        result.add(Code.class.cast(v).getCode());
                    } else {
                        result.add(v.toString());
                    }
                });
                return result;
            }
            break;
        case LONG:
            // maybe need clone?
            result = array;
            // result = array.stream().map(Long.class::cast).collect(Collectors.toList());
            break;
        case FLOAT:
            result = array;
            // Mongo DB document don't have float type, so all double type
            // result = array.stream().map(Double.class::cast).collect(Collectors.toList());
            break;
        case DOUBLE:
            result = array;
            // result = array.stream().map(Double.class::cast).collect(Collectors.toList());
            break;
        case BOOLEAN:
            result = array;
            // result = array.stream().map(Boolean.TRUE::equals).collect(Collectors.toList());
            break;
        case INT: {
            result = array;
            // result = array.stream().map(Integer.class::cast).collect(Collectors.toList());
            break;
        }
        case BYTES: {
            // TODO use default encoding? not UTF8
            result = array.stream().map(String.class::cast).map(v -> v.getBytes()).collect(Collectors.toList());
            break;
        }
        case DATETIME: {
            result = array;
            // result = array.stream().map(Date.class::cast).collect(Collectors.toList());
            break;
        }
        default: {
            result = array;
        }
        }

        return result;
    }

    private Type translateType(Object value, DatatypeHolder data_type_holder) {
        if (value instanceof String) {
            return Type.STRING;
        } else if (value instanceof Integer) {
            return Type.INT;
        } else if (value instanceof Long) {
            return Type.LONG;
        } else if (value instanceof Short) {
            return Type.INT;
        } else if (value instanceof Double) {
            return Type.DOUBLE;
        } else if (value instanceof Float) {
            return Type.FLOAT;
        } else if (value instanceof Boolean) {
            return Type.BOOLEAN;
        } else if (value instanceof Date) {
            return Type.DATETIME;
        } else if (value instanceof ObjectId) {
            data_type_holder.data_type = DataType.OBJECTID;
            return Type.STRING;
        } else if (value instanceof Code) {
            data_type_holder.data_type = DataType.CODE;
            return Type.STRING;
        } else if (value instanceof Decimal128) {
            data_type_holder.data_type = DataType.DECIMAL128;
            return Type.STRING;
        } else {
            return Type.STRING;
        }
    }

    enum DataType {

        // special for MongoDB
        OBJECTID("objectid"),
        CODE("code"),
        DECIMAL128("decimal128");

        final String origin_type;

        DataType(String origin_type) {
            this.origin_type = origin_type;
        }

    }

    class DatatypeHolder {

        DataType data_type;
    }
}
