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

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TCKRecordConverter extends GroupConverter {

    private final Consumer<Record> recordSetter;

    private final RecordBuilderFactory factory;

    private final Schema schemaTCK;

    private Record.Builder recordBuilder;

    private final Converter[] converters;

    public TCKRecordConverter(RecordBuilderFactory factory, Consumer<Record> recordSetter, GroupType parquetType,
            final Schema tckParentType) {
        this.factory = factory;
        this.recordSetter = recordSetter;
        this.converters = new Converter[parquetType.getFieldCount()];
        final SchemaReader reader = new SchemaReader(this.factory);
        this.schemaTCK = reader.convert(parquetType);
        for (int i = 0; i < parquetType.getFieldCount(); i++) {
            final Type fieldType = parquetType.getType(i);
            final Converter converter = TCKConverter.buildConverter(fieldType, this.factory, this.schemaTCK,
                    () -> this.recordBuilder);
            this.converters[i] = converter;
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return this.converters[fieldIndex];
    }

    @Override
    public void start() {
        log.info("start");
        this.recordBuilder = this.factory.newRecordBuilder(this.schemaTCK);
    }

    @Override
    public void end() {
        log.info("end");
        for (Converter converter : this.converters) {
            if (converter instanceof TCKArrayPrimitiveConverter) {
                ((TCKArrayPrimitiveConverter) converter).end();
            }
        }
        final Record record = this.recordBuilder.build();
        this.recordSetter.accept(record);
    }
}
