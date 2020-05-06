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
package org.talend.components.rabbitmq.configuration;

import java.io.Serializable;

import org.talend.components.rabbitmq.datastore.RabbitMQDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@DataSet("RabbitMQDataSet")
@Data
@GridLayout(value = { @GridLayout.Row({ "connection" }), @GridLayout.Row({ "receiverType" }), @GridLayout.Row({ "queue" }),
        @GridLayout.Row({ "exchangeType" }), @GridLayout.Row({ "exchange" }),
        @GridLayout.Row({ "routingKey" }) }, names = GridLayout.FormType.MAIN)
@GridLayout(value = { @GridLayout.Row({ "durable" }), @GridLayout.Row({ "autoDelete" }) }, names = GridLayout.FormType.ADVANCED)
public class BasicConfiguration implements Serializable {

    @Option
    @Documentation("RabbitMQ connection information")
    private RabbitMQDataStore connection;

    @Option
    @Documentation("Message Type list")
    private ReceiverType receiverType = ReceiverType.QUEUE;

    @Option
    @ActiveIf(target = "receiverType", value = "QUEUE")
    @Documentation("QUEUE Name")
    private String queue;

    @Option
    @ActiveIf(target = "receiverType", value = "EXCHANGE")
    @Documentation("EXCHANGE Type")
    private ExchangeType exchangeType = ExchangeType.FANOUT;

    @Option
    @ActiveIf(target = "receiverType", value = "EXCHANGE")
    @Documentation("EXCHANGE Name")
    private String exchange;

    @Option
    @ActiveIfs(value = { @ActiveIf(target = "exchangeType", value = { "DIRECT", "TOPIC" }),
            @ActiveIf(target = "receiverType", value = { "EXCHANGE" }) }, operator = ActiveIfs.Operator.AND)
    @Documentation("Routing key")
    private String routingKey;

    @Option
    @Documentation("true if we are declaring a durable queue (the queue will survive a server restart)")
    private Boolean durable = false;

    @Option
    @Documentation("true if we are declaring an autodelete queue (server will delete it when no longer in use)")
    private Boolean autoDelete = false;

    public String getRoutingKey() {
        return (ExchangeType.DIRECT == exchangeType || ExchangeType.TOPIC == exchangeType)
                && ReceiverType.EXCHANGE == receiverType ? routingKey : "";
    }
}
