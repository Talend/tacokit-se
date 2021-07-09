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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.talend.components.common.stream.format.parquet.Constants;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TCKArrayConverter extends GroupConverter {

    private final Consumer<Collection<Object>> arraySetter;

    private final Converter converter;

    private List<Object> values = new ArrayList<>();

    public TCKArrayConverter(final Consumer<Collection<Object>> arraySetter, final RecordBuilderFactory factory,
            final org.apache.parquet.schema.Type parquetType, final Schema tckType) {

        this.arraySetter = arraySetter;
        if (parquetType.isPrimitive()) {
            this.converter = new TCKPrimitiveConverter(this::addValue);
        } else {
            final GroupType groupType = parquetType.asGroupType();
            final Type type = TCKConverter.innerArrayType(parquetType);
            if (type != null) {
                if (type.isPrimitive()) {
                    final Collection<Object> subObject = new ArrayList<>();
                    final TCKPrimitiveConverter primitiveConverter = new TCKPrimitiveConverter(subObject::add);

                    // this.converter = new TCKNoActionConverter(new TCKArrayConverter(this::addValue, factory, type,
                    // tckType.getElementSchema()));
                    this.converter = new TCKEndActionConverter(new TCKEndActionConverter(primitiveConverter), () -> {
                        ArrayList<Object> copy = new ArrayList<>(subObject.size());
                        copy.addAll(subObject);
                        TCKArrayConverter.this.addValue(copy);
                        subObject.clear();
                    });
                } else {
                    final Name name = Name.fromParquetName(type.getName());
                    final Schema elementSchema = tckType.getElementSchema();
                    final Schema.Entry.Builder fieldBuilder = factory.newEntryBuilder().withName(name.getName())
                            .withNullable(true).withType(elementSchema.getType());
                    if (elementSchema.getElementSchema() != null) {
                        fieldBuilder.withElementSchema(elementSchema.getElementSchema());
                    }
                    if (elementSchema.getType() == Schema.Type.RECORD) {
                        fieldBuilder.withElementSchema(elementSchema);
                    }
                    final Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(fieldBuilder.build()).build();
                    final Record.Builder subRecordBuilder[] = new Record.Builder[1];
                    subRecordBuilder[0] = factory.newRecordBuilder(schema);

                    final Collection<Object> elements = new ArrayList<>();
                    final Converter buildConverter = TCKConverter.buildConverter(type, factory, schema,
                            () -> subRecordBuilder[0]);
                    final TCKEndActionConverter actionConverter = new TCKEndActionConverter(buildConverter, () -> {
                        final Record record = subRecordBuilder[0].build();
                        final Object value = record.get(Object.class, name.getName());
                        elements.add(value);
                        subRecordBuilder[0] = factory.newRecordBuilder(schema);
                    });
                    this.converter = new TCKEndActionConverter(actionConverter, () -> {
                        ArrayList<Object> copy = new ArrayList<>(elements.size());
                        copy.addAll(elements);
                        TCKArrayConverter.this.addValue(copy);
                        elements.clear();
                    });
                }
            } else {
                this.converter = new TCKRecordConverter(factory, this::addValue, groupType, tckType);
            }
        }
    }

    private void addValue(Object value) {
        this.values.add(value);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return this.converter;
    }

    @Override
    public void start() {
        log.info("start");
    }

    @Override
    public void end() {
        log.info("end, array size " + values.size());
        ArrayList<Object> copy = new ArrayList<>(values.size());
        copy.addAll(values);
        this.arraySetter.accept(copy);
        this.values.clear();
    }

}
