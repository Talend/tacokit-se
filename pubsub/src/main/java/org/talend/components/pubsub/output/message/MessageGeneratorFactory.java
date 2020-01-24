/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.input.converter.TextMessageConverter;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.service.record.RecordService;

import java.util.Arrays;
import java.util.Optional;

public class MessageGeneratorFactory {

    private static final Class<? extends MessageGenerator>[] IMPLEMENTATIONS = new Class[] { AvroMessageGenerator.class,
            CSVMessageGenerator.class, JSONMessageGenerator.class, TextMessageConverter.class };

    public MessageGenerator getGenerator(PubSubDataSet dataset, I18nMessage i18n, RecordService recordService) {
        PubSubDataSet.ValueFormat format = dataset.getValueFormat();

        Optional<? extends MessageGenerator> opt = Arrays.stream(IMPLEMENTATIONS).map(c -> {
            try {
                return c.newInstance();
            } catch (Exception e) {
                return null;
            }
        }).filter(mg -> mg != null && ((MessageGenerator) mg).acceptFormat(format)).findFirst();

        MessageGenerator messageGenerator = opt.isPresent() ? opt.get() : new TextMessageGenerator();

        messageGenerator.setI18nMessage(i18n);
        messageGenerator.setRecordService(recordService);
        messageGenerator.init(dataset);

        return messageGenerator;

    }
}
