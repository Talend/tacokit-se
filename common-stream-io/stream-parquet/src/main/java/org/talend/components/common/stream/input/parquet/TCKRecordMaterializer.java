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
package org.talend.components.common.stream.input.parquet;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.talend.components.common.stream.input.parquet.converter.TCKRecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.AccessLevel;
import lombok.Setter;

public class TCKRecordMaterializer extends RecordMaterializer<Record> {

    @Setter(AccessLevel.PRIVATE)
    private Record currentRecord;

    private TCKRecordConverter converter;

    public TCKRecordMaterializer(RecordBuilderFactory factory, MessageType parquetSchema) {
        this.converter = new TCKRecordConverter(factory, this::setCurrentRecord, parquetSchema, null);
    }

    @Override
    public Record getCurrentRecord() {
        return this.currentRecord;
    }

    @Override
    public GroupConverter getRootConverter() {
        return this.converter;
    }
}
