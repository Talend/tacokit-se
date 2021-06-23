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

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.components.common.stream.input.parquet.converter.TCKArrayPrimitiveConverter.CollectionSetter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;

public class TCKConverter {

    public static Converter buildConverter(final org.apache.parquet.schema.Type parquetType,
            final RecordBuilderFactory factory,
            final Schema tckType,
            final Schema tckParentType,
            final Supplier<Record.Builder> builderGetter,
            final org.apache.parquet.schema.Type parent) {

        final Name name = Name.fromParquetName(parquetType.getName());
        final Schema.Entry field = tckType.getEntry(name.getName());

        if (TCKConverter.isArrayEncapsulated(parquetType)) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter.setArray(elements, builderGetter, field);
            final Type elementField = parquetType.asGroupType().getFields().get(0);
            final TCKArrayConverter arrayConverter =
                    new TCKArrayConverter(collectionSetter, factory, elementField, tckType);
            return arrayConverter;
        }

        if (parquetType.isRepetition(Repetition.REPEATED)) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> TCKConverter.setArrayPrimitive(elements, builderGetter, field);
            return new TCKArrayPrimitiveConverter(new CollectionSetter(collectionSetter, field));
        }
        if (parquetType.isPrimitive()) {
            final Consumer<Object> valueSetter = (Object value) -> TCKConverter.setObject(value, builderGetter, field);
            return new TCKPrimitiveConverter(valueSetter);
        }

        // TCK Record.
        final Consumer<Record> recordSetter = (Record rec) ->  TCKConverter.setRecord( rec, builderGetter, field); // builderGetter.get().withRecord(field, rec);
        return new TCKRecordConverter(factory, recordSetter, parquetType.asGroupType(), tckType);
    }

    private static void setArray(Collection<Object> elements, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        builderGetter.get().withArray(field, elements);
    }

    private static void setArrayPrimitive(Collection<Object> elements, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        builderGetter.get().withArray(field, elements);
    }

    private static void setObject(Object value, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        final Object realValue = TCKConverter.realValue(field.getType(), value);
        builderGetter.get().with(field,  realValue);
    }

    private static void setRecord(Record rec, final Supplier<Record.Builder> builderGetter, final Schema.Entry field) {
        builderGetter.get().withRecord(field, rec);
    }

    public static Object realValue(final Schema.Type fieldType, Object value) {
        if (fieldType == Schema.Type.STRING && value instanceof Binary) {
            return ((Binary) value).toStringUsingUTF8();
        }
        return value;
    }

    private static boolean isArrayEncapsulated(final org.apache.parquet.schema.Type parquetField) {
        if (parquetField.isPrimitive()) {
            return false;
        }

        final List<Type> fields = parquetField.asGroupType().getFields();
        if (fields != null && fields.size() == 1) {
            final org.apache.parquet.schema.Type elementType = fields.get(0);
            if (!elementType.isPrimitive() && "list".equals(elementType.getName())) {
                return elementType.getRepetition() == Repetition.REPEATED;
            }
        }
        return false;
    }

}
