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
package org.talend.components.activemq.service;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.talend.components.activemq.configuration.MessageType;
import org.talend.components.activemq.datastore.ActiveMQDataStore;
import org.talend.sdk.component.api.service.Service;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ActiveMQService {

    @Service
    private I18nMessage i18n;

    public Destination getDestination(Session session, String destination, MessageType messageType) throws JMSException {
        return (MessageType.QUEUE == messageType) ? session.createQueue(destination) : session.createTopic(destination);
    }

    public ConnectionFactory createConnectionFactory(ActiveMQDataStore dataStore) {
        log.info(dataStore.getUrl());
        return new ActiveMQConnectionFactory(dataStore.getUrl());
    }

    public Session getSession(Connection connection, Boolean transacted) throws JMSException {
        int acknowledge = transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE;
        return connection.createSession(transacted, acknowledge);
    }

    public Connection getConnection(ConnectionFactory connectionFactory, ActiveMQDataStore dataStore) throws JMSException {
        return dataStore.isUserIdentity() ? connectionFactory.createConnection(dataStore.getUserName(), dataStore.getPassword())
                : connectionFactory.createConnection();
    }

    public void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException e) {
                log.warn(i18n.warnConnectionCantBeClosed(), e);
            }
        }
    }

    public void commit(Session session) {
        try {
            if (session.getTransacted()) {
                session.commit();
            }
        } catch (JMSException e) {

            try {
                session.rollback();
            } catch (JMSException e1) {
                log.error("Can't rollback", e);
            }
        }
    }

}
