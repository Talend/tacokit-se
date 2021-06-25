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
package org.talend.components.common.stream.input.parquet.converter;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.talend.components.common.stream.format.parquet.Constants;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.components.common.stream.input.parquet.converter.TCKArrayPrimitiveConverter.CollectionSetter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TCKConverter {

    public static Converter buildConverter(final org.apache.parquet.schema.Type parquetType, final RecordBuilderFactory factory,
            final Schema tckType, final Supplier<Record.Builder> builderGetter) {

        final Name name = Name.fromParquetName(parquetType.getName());
        final Schema.Entry field = tckType.getEntry(name.getName());

        final Type innerArrayType = TCKConverter.innerArrayType(parquetType);
        if (innerArrayType != null && (!innerArrayType.isPrimitive())) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter.setArray(elements,
                    builderGetter, field);
            final TCKArrayConverter arrayConverter = new TCKArrayConverter(collectionSetter, factory, innerArrayType,
                    field.getElementSchema());
            return new TCKEndActionConverter(arrayConverter); // for list
        }
        if (innerArrayType != null && innerArrayType.isPrimitive()) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter
                    .setArrayPrimitive(elements, builderGetter, field);
            final TCKArrayPrimitiveConverter pconv = new TCKArrayPrimitiveConverter(
                    new CollectionSetter(collectionSetter, field));
            return new TCKEndActionConverter(new TCKEndActionConverter(pconv));
        }
        if (parquetType.isPrimitive()) {

            if (parquetType.isRepetition(Repetition.REPEATED)) {
                final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter
                        .setArrayPrimitive(elements, builderGetter, field);
                return new TCKArrayPrimitiveConverter(new CollectionSetter(collectionSetter, field));
            }
            final Consumer<Object> valueSetter = (Object value) -> TCKConverter.setObject(value, builderGetter, field);
            return new TCKPrimitiveConverter(valueSetter);
        }

        // TCK Record.
        if (parquetType.isRepetition(Repetition.REPEATED)) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter.setArray(elements,
                    builderGetter, field);
            final TCKArrayConverter arrayConverter = new TCKArrayConverter(collectionSetter, factory, parquetType,
                    field.getElementSchema());
            return new TCKEndActionConverter(arrayConverter); // for list
        }
        final Consumer<Record> recordSetter = (Record rec) -> TCKConverter.setRecord(rec, builderGetter, field); // builderGetter.get().withRecord(field,
                                                                                                                 // rec);
        TCKRecordConverter rec = new TCKRecordConverter(factory, recordSetter, parquetType.asGroupType(), tckType);
        return rec;
    }

    private static void setArray(Collection<Object> elements, final Supplier<Record.Builder> builderGetter,
            final Schema.Entry field) {
        final Record.Builder builder = builderGetter.get();
        builder.withArray(field, elements);
    }

    private static void setArrayPrimitive(Collection<Object> elements, final Supplier<Record.Builder> builderGetter,
            final Schema.Entry field) {
        log.info("Array add : " + elements.size());
        builderGetter.get().withArray(field, elements);
    }

    private static void setObject(final Object value, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        final Object realValue = TCKConverter.realValue(field.getType(), value);
        if (field.getType() == Schema.Type.BYTES && realValue instanceof String) {
            builderGetter.get().with(field, ((String) realValue).getBytes(StandardCharsets.UTF_8));
        } else {
            builderGetter.get().with(field, realValue);
        }
    }

    private static void setRecord(final Record rec, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        if (rec != null) {
            builderGetter.get().withRecord(field, rec);
        }
    }

    public static Object realValue(final Schema.Type fieldType, Object value) {
        if (fieldType == Schema.Type.STRING && value instanceof Binary) {
            return ((Binary) value).toStringUsingUTF8();
        }
        return value;
    }

    public static org.apache.parquet.schema.Type innerArrayType(final org.apache.parquet.schema.Type parquetField) {
        if (parquetField.isPrimitive()) {
            return null;
        }

        org.apache.parquet.schema.Type listParquet = null;
        if (Constants.LIST_NAME.equals(parquetField.getName())) {
            listParquet = parquetField;
        } else {
            final List<Type> fields = parquetField.asGroupType().getFields();
            if (fields != null && fields.size() == 1) {
                final org.apache.parquet.schema.Type elementType = fields.get(0);
                if (Constants.LIST_NAME.equals(elementType.getName())) {
                    listParquet = elementType;
                }
            }
        }
        if (listParquet != null && listParquet.isRepetition(Repetition.REPEATED) && (!listParquet.isPrimitive())) {
            final List<Type> elementFields = listParquet.asGroupType().getFields();
            if (elementFields != null && elementFields.size() == 1) {
                return elementFields.get(0);
            }
        }

        return null;
    }

}
