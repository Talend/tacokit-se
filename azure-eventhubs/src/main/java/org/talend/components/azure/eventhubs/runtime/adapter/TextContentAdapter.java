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
package org.talend.components.azure.eventhubs.runtime.adapter;

import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_CHARSET;

import org.talend.components.azure.eventhubs.runtime.converters.TextConverter;
import org.talend.components.azure.eventhubs.service.Messages;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TextContentAdapter implements EventDataContentAdapter {

    private final TextConverter recordConverter;

    public TextContentAdapter(RecordBuilderFactory recordBuilderFactory, Messages messages) {
        recordConverter = TextConverter.of(recordBuilderFactory, messages);
    }

    @Override
    public Record toRecord(byte[] event) {
        return recordConverter.toRecord(new String(event, DEFAULT_CHARSET));
    }

    @Override
    public byte[] toBytes(Record record) {
        return recordConverter.fromRecord(record).getBytes(DEFAULT_CHARSET);
    }
}
