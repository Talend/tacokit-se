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
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.schema.Type.Repetition;
import org.talend.components.common.stream.input.parquet.converter.TCKArrayPrimitiveConverter.CollectionSetter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class TCKConverter {

    public static Converter buildConverter(final org.apache.parquet.schema.Type parquetType, final RecordBuilderFactory factory,
            final Schema tckType, final Supplier<Record.Builder> builderGetter) {

        final Schema.Entry field = tckType.getEntry(parquetType.getName());

        if (parquetType.isRepetition(Repetition.REPEATED) && !parquetType.isPrimitive()) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> builderGetter.get()
                    .withArray(field, elements);
            return new TCKArrayConverter(collectionSetter, factory, parquetType);
        }
        if (parquetType.isRepetition(Repetition.REPEATED)) {
            final Consumer<Collection<Object>> collectionSetter = (Collection<Object> elements) -> builderGetter.get()
                    .withArray(field, elements);
            return new TCKArrayPrimitiveConverter(new CollectionSetter(collectionSetter, field));
        }
        if (parquetType.isPrimitive()) {
            final Consumer<Object> valueSetter = (Object value) -> builderGetter.get().with(field,
                    TCKConverter.realValue(field.getType(), value));
            return new TCKPrimitiveConverter(valueSetter);
        }

        // TCK Record.
        final Consumer<Record> recordSetter = (Record rec) -> builderGetter.get().withRecord(field, rec);
        return new TCKRecordConverter(factory, recordSetter, parquetType.asGroupType());
    }

    public static Object realValue(final Schema.Type fieldType, Object value) {
        if (fieldType == Schema.Type.STRING && value instanceof Binary) {
            return ((Binary) value).toStringUsingUTF8();
        }
        return value;
    }

}
