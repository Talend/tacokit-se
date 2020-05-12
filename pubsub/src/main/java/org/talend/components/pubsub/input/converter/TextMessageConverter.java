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
package org.talend.components.pubsub.input.converter;

import com.google.pubsub.v1.PubsubMessage;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

/**
 * {@link MessageConverter} that accepts any message.
 */
public class TextMessageConverter extends MessageConverter {

    private Schema schema;

    @Override
    public void init(PubSubDataSet dataset) {
        schema = getRecordBuilderFactory().newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(getRecordBuilderFactory().newEntryBuilder().withName("ID").withType(Schema.Type.STRING).build())
                .withEntry(getRecordBuilderFactory().newEntryBuilder().withName("content").withType(Schema.Type.STRING)
                        .withNullable(true).build())
                .build();
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return true;
    }

    @Override
    public Record convertMessage(PubsubMessage message) {
        return getRecordBuilderFactory().newRecordBuilder(schema).withString("ID", message.getMessageId())
                .withString("content", getMessageContentAsString(message)).build();
    }
}
