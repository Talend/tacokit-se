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
package org.talend.components.pubsub.input;

import com.google.gson.internal.Streams;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.input.converter.MessageConverterFactory;
import org.talend.components.pubsub.service.AckMessageService;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@PartitionMapper(name = "PubSubInput", infinite = true)
@Documentation("This component listens to a PubSub topic.")
@Slf4j
@RequiredArgsConstructor
public class PubSubPartitionMapper implements Serializable {

    protected final PubSubInputConfiguration configuration;

    protected final PubSubService service;

    protected final AckMessageService ackMessageService;

    protected final I18nMessage i18n;

    protected final RecordBuilderFactory builderFactory;

    protected final MessageConverterFactory messageConverterFactory;

    @Assessor
    public long estimateSize() {
        return 1l;
    }

    @Split
    public List<PubSubPartitionMapper> split(@PartitionSize final int desiredNbSplits) {
        String subscription = configuration.getDataSet().getSubscription();
        if (subscription == null || "".equals(subscription.trim())) {
            subscription = "s" + UUID.randomUUID().toString();
            configuration.getDataSet().setSubscription(subscription);
        }

        return IntStream.range(0, desiredNbSplits).mapToObj(i -> new PubSubPartitionMapper(configuration, service,
                ackMessageService, i18n, builderFactory, messageConverterFactory)).collect(Collectors.toList());
    }

    @Emitter
    public PubSubInput createSource() {
        return new PubSubInput(configuration, service, ackMessageService, i18n, builderFactory, messageConverterFactory);
    }

}
