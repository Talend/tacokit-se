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
package org.talend.components.pubsub.output;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.output.message.MessageGenerator;
import org.talend.components.pubsub.output.message.MessageGeneratorFactory;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@Processor(name = "PubSubOutput")
@Documentation("This component sends messages to a Pub/Sub topic.")
public class PubSubOutput implements Serializable {

    private final I18nMessage i18n;

    private final PubSubOutputConfiguration configuration;

    private final RecordService recordService;

    private transient boolean init;

    private transient Publisher publisher;

    private final PubSubService pubSubService;

    private transient MessageGenerator messageGenerator;

    public PubSubOutput(@Option("configuration") final PubSubOutputConfiguration configuration, PubSubService pubSubService,
            I18nMessage i18n, RecordService recordService) {
        this.configuration = configuration;
        this.i18n = i18n;
        this.pubSubService = pubSubService;
        this.recordService = recordService;
    }

    @PostConstruct
    public void init() {

    }

    @ElementListener
    public void onElement(Record record) {
        if (!init) {
            lazyInit();
        }
        PubsubMessage message = messageGenerator.generateMessage(record);
        publisher.publish(message);
    }

    private void lazyInit() {
        init = true;
        if (configuration.getTopicOperation() == PubSubOutputConfiguration.TopicOperation.DROP_IF_EXISTS_AND_CREATE) {
            pubSubService.removeTopicIfExists(configuration.getDataset().getDataStore(), configuration.getDataset().getTopic());
            pubSubService.createTopicIfNeeded(configuration.getDataset().getDataStore(), configuration.getDataset().getTopic());
        } else if (configuration.getTopicOperation() == PubSubOutputConfiguration.TopicOperation.CREATE_IF_NOT_EXISTS) {
            pubSubService.createTopicIfNeeded(configuration.getDataset().getDataStore(), configuration.getDataset().getTopic());
        }
        publisher = pubSubService.createPublisher(configuration.getDataset().getDataStore(),
                configuration.getDataset().getTopic());

        messageGenerator = new MessageGeneratorFactory().getGenerator(configuration.getDataset(), i18n, recordService);
    }

    @PreDestroy
    public void release() {
        if (publisher != null) {
            publisher.shutdown();
        }
    }

}
