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

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TCKReadSupport extends ReadSupport<Record> {

    private final RecordMaterializer<Record> materializer;

    private final MessageType messageType;

    public TCKReadSupport(final RecordBuilderFactory factory, MessageType messageType) {
        this(new TCKRecordMaterializer(factory, messageType), //
                messageType);
    }

    @Override
    public RecordMaterializer<Record> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
            MessageType fileSchema, ReadContext readContext) {
        return materializer;
    }

    @Override
    public org.apache.parquet.hadoop.api.ReadSupport.ReadContext init(InitContext context) {
        return new ReadContext(messageType);
    }

}
