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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;
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
            this.converter = new TCKPrimitiveConverter(this.values::add);
        } else {
            final GroupType groupType = parquetType.asGroupType();
            this.converter = new TCKRecordConverter(factory, this::addValue, groupType, tckType);
        }
    }

    private void addValue(Object value) {
        log.info("add value " + value);
        boolean done = false;
        if (value instanceof Record) {
            final Record rec = (Record) value;
            try {
                final Record element = rec.getRecord("element");
                if (element != null) {
                    this.values.add(element);
                }
                done = true;
            }
            catch (RuntimeException ex) {
            }
        }
        if (!done) {
            this.values.add(value);
        }
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
        this.arraySetter.accept(this.values);
        this.values = new ArrayList<>();
    }

}
