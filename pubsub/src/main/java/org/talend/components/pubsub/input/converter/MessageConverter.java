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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.input.PubSubInputConfiguration;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public abstract class MessageConverter {

    @Setter
    @Getter(value = AccessLevel.PROTECTED)
    private RecordBuilderFactory recordBuilderFactory;

    @Setter
    @Getter(value = AccessLevel.PROTECTED)
    private I18nMessage i18nMessage;

    public void init(PubSubDataSet dataset) {

    }

    public abstract boolean acceptFormat(PubSubDataSet.ValueFormat format);

    public abstract Object convertMessage(PubsubMessage message);

    protected final String getMessageContentAsString(PubsubMessage message) {
        return message == null ? "null" : message.getData().toStringUtf8();
    }

    protected final byte[] getMessageContentAsBytes(PubsubMessage message) {
        return message == null ? new byte[] {} : message.getData().toByteArray();
    }
}
