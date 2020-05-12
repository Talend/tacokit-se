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

import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.util.Arrays;
import java.util.Optional;

@Slf4j
@Service
public class MessageConverterFactory {

    private static final Class<? extends MessageConverter>[] IMPLEMENTATIONS = new Class[] { CSVMessageConverter.class,
            AvroMessageConverter.class, JSonMessageConverter.class, TextMessageConverter.class };

    public MessageConverter getConverter(PubSubDataSet dataSet, RecordBuilderFactory recordBuilderFactory, I18nMessage i18n) {
        PubSubDataSet.ValueFormat format = dataSet.getValueFormat();

        Optional<? extends MessageConverter> opt = Arrays.stream(IMPLEMENTATIONS).map(c -> {
            try {
                return c.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                return null;
            }
        }).filter(mc -> mc != null && ((MessageConverter) mc).acceptFormat(format)).findFirst();

        MessageConverter messageConverter = opt.isPresent() ? opt.get() : new TextMessageConverter();

        messageConverter.setRecordBuilderFactory(recordBuilderFactory);
        messageConverter.setI18nMessage(i18n);
        messageConverter.init(dataSet);

        return messageConverter;
    }
}
