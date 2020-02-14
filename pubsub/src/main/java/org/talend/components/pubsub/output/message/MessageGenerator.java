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

import com.google.pubsub.v1.PubsubMessage;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordService;

public abstract class MessageGenerator {

    @Setter
    @Getter(value = AccessLevel.PROTECTED)
    private I18nMessage i18nMessage;

    @Setter
    @Getter(value = AccessLevel.PROTECTED)
    private RecordService recordService;

    @Setter
    @Getter(value = AccessLevel.PROTECTED)
    private RecordIORepository ioRepository;

    public abstract void init(PubSubDataSet dataset);

    public abstract PubsubMessage generateMessage(Record record);

    public abstract boolean acceptFormat(PubSubDataSet.ValueFormat format);
}
