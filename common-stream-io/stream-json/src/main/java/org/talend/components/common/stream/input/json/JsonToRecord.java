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
package org.talend.components.common.stream.input.json;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class JsonToRecord {

    private final RecordBuilderFactory factory;

    public JsonToRecord(final RecordBuilderFactory factory) {
        this.factory = factory;
    }

    /*
     * Copy from TCK RecordConverters.java
     * Just removing dependency to JsonLorg.apache.johnzon.core.JsonLongImpl
     * https://github.com/Talend/component-runtime/blob/0597e8dc0498559528a65cde64eccfe1cfea2913/component-runtime-impl/src/main/
     * java/org/talend/sdk/component/runtime/record/RecordConverters.java#L134
     */
    public Record toRecord(final JsonObject object) {
        final Record.Builder builder = factory.newRecordBuilder();
        object.forEach((String key, JsonValue value) -> {
            switch (value.getValueType()) {
            case ARRAY: {
                final List<Object> items = value.asJsonArray().stream().map(this::mapJson).collect(toList());
                builder.withArray(factory.newEntryBuilder().withName(key).withType(Schema.Type.ARRAY)
                        .withElementSchema(getArrayElementSchema(factory, items)).build(), items);
                break;
            }
            case OBJECT: {
                final Record record = toRecord(value.asJsonObject());
                builder.withRecord(factory.newEntryBuilder().withName(key).withType(Schema.Type.RECORD)
                        .withElementSchema(record.getSchema()).build(), record);
                break;
            }
            case TRUE:
            case FALSE:
                builder.withBoolean(key, JsonValue.TRUE.equals(value));
                break;
            case STRING:
                builder.withString(key, JsonString.class.cast(value).getString());
                break;
            case NUMBER:
                final JsonNumber number = JsonNumber.class.cast(value);
                builder.withDouble(key, number.doubleValue());
                break;
            case NULL:
                break;
            default:
                throw new IllegalArgumentException("Unsupported value type: " + value);
            }
        });
        return builder.build();
    }

    private Object mapJson(final JsonValue it) {
        if (JsonObject.class.isInstance(it)) {
            return toRecord(it.asJsonObject());
        }
        if (JsonArray.class.isInstance(it)) {
            return it.asJsonArray().stream().map(this::mapJson).collect(toList());
        }
        if (JsonString.class.isInstance(it)) {
            return JsonString.class.cast(it).getString();
        }
        if (JsonNumber.class.isInstance(it)) {
            return JsonNumber.class.cast(it).numberValue();
        }
        if (JsonValue.FALSE.equals(it)) {
            return false;
        }
        if (JsonValue.TRUE.equals(it)) {
            return true;
        }
        if (JsonValue.NULL.equals(it)) {
            return null;
        }
        return it;
    }

    private Schema getArrayElementSchema(final RecordBuilderFactory factory, final List<Object> items) {
        if (items.isEmpty()) {
            return factory.newSchemaBuilder(Schema.Type.STRING).build();
        }
        final Schema firstSchema = toSchema(items.get(0));
        if (firstSchema.getType() == Schema.Type.RECORD) {
            // This code merges schema of all record of the array [{aaa, bbb}, {aaa, ccc}] => {aaa, bbb, ccc}
            return items.stream().map(it -> toSchema(it)).reduce(null, (Schema s1, Schema s2) -> {
                if (s1 == null) {
                    return s2;
                }
                if (s2 == null) { // unlikely
                    return s1;
                }
                final List<Schema.Entry> entries1 = s1.getEntries();
                final List<Schema.Entry> entries2 = s2.getEntries();
                final Set<String> names1 = entries1.stream().map(Schema.Entry::getName).collect(toSet());
                final Set<String> names2 = entries2.stream().map(Schema.Entry::getName).collect(toSet());
                if (!names1.equals(names2)) {
                    // here we are not good since values will not be right anymore,
                    // forbidden for current version anyway but potentially supported later
                    final Schema.Builder builder = factory.newSchemaBuilder(Schema.Type.RECORD);
                    entries1.forEach(builder::withEntry);
                    entries2.stream().filter(it -> !names1.contains(it.getName())).forEach(builder::withEntry);
                    return builder.build();
                }
                return s1;
            });
        } else {
            return firstSchema;
        }
    }

    private Schema toSchema(final Object next) {
        if (String.class.isInstance(next) || JsonString.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.STRING).build();
        }
        if (Integer.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.INT).build();
        }
        if (Long.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.LONG).build();
        }
        if (Float.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.FLOAT).build();
        }
        if (BigDecimal.class.isInstance(next) || JsonNumber.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.DOUBLE).build();
        }
        if (Double.class.isInstance(next) || JsonNumber.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.DOUBLE).build();
        }
        if (Boolean.class.isInstance(next) || JsonValue.TRUE.equals(next) || JsonValue.FALSE.equals(next)) {
            return factory.newSchemaBuilder(Schema.Type.BOOLEAN).build();
        }
        if (Date.class.isInstance(next) || ZonedDateTime.class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.DATETIME).build();
        }
        if (byte[].class.isInstance(next)) {
            return factory.newSchemaBuilder(Schema.Type.BYTES).build();
        }
        if (Collection.class.isInstance(next) || JsonArray.class.isInstance(next)) {
            final Collection collection = Collection.class.cast(next);
            if (collection.isEmpty()) {
                return factory.newSchemaBuilder(Schema.Type.STRING).build();
            }
            return factory.newSchemaBuilder(Schema.Type.ARRAY).withElementSchema(toSchema(collection.iterator().next())).build();
        }
        if (Record.class.isInstance(next)) {
            return Record.class.cast(next).getSchema();
        }
        throw new IllegalArgumentException("unsupported type for " + next);
    }
}
