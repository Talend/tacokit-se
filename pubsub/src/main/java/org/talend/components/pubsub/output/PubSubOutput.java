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
package org.talend.components.pubsub.output;

import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.pubsub.output.message.MessageGenerator;
import org.talend.components.pubsub.output.message.MessageGeneratorFactory;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.components.pubsub.service.PubSubConnectorException;
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
import java.util.concurrent.TimeUnit;

@Slf4j
@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@Processor(name = "PubSubOutput")
@Documentation("This component sends messages to a Pub/Sub topic.")
@RequiredArgsConstructor
public class PubSubOutput implements Serializable {

    private final I18nMessage i18n;

    private final PubSubOutputConfiguration configuration;

    private final MessageGeneratorFactory messageGeneratorFactory;

    private transient boolean init;

    private transient Publisher publisher;

    private final PubSubService pubSubService;

    private transient MessageGenerator messageGenerator;

    @ElementListener
    public void onElement(Record record) {
        if (!init) {
            lazyInit();
        }
        PubsubMessage message = messageGenerator.generateMessage(record);
        if (message != null) {
            publisher.publish(message);
        }
    }

    private void lazyInit() {
        init = true;
        Topic topic = pubSubService.loadTopic(configuration.getDataset().getDataStore(), configuration.getDataset().getTopic());
        if (topic == null) {
            throw new PubSubConnectorException(i18n.topicDoesNotExist(configuration.getDataset().getTopic()));
        }
        publisher = pubSubService.createPublisher(configuration.getDataset().getDataStore(),
                configuration.getDataset().getTopic());

        messageGenerator = messageGeneratorFactory.getGenerator(configuration.getDataset());
    }

    @PreDestroy
    public void release() {
        if (publisher != null) {
            try {
                publisher.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error(e.getMessage(), e);
            }
            publisher.shutdown();
        }
    }

}
