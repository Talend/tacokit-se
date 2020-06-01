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
package org.talend.components.pubsub.output.message;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointer;
import org.talend.sdk.component.api.record.RecordPointerFactory;
import org.talend.sdk.component.api.service.Service;

@Slf4j
public class TextMessageGenerator extends MessageGenerator {

    @Service
    @Setter(AccessLevel.PROTECTED)
    private RecordPointerFactory recordPointerFactory;

    private RecordPointer recordPointer;

    @Override
    public void init(PubSubDataSet dataset) {
        recordPointer = recordPointerFactory.apply(dataset.getPathToText());
    }

    @Override
    public PubsubMessage generateMessage(Record record) {
        try {
            String text = recordPointer.getValue(record, Object.class).toString();
            return PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(text)).build();
        } catch (IllegalArgumentException iae) {
            log.info(getI18nMessage().infoNoTextContent(iae.getMessage()));
            return null;
        }
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return true;
    }
}
