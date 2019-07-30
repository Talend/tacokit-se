/**
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.components.mail.output;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.io.IOException;

import javax.mail.Message;
import javax.mail.MessagingException;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.icegreen.greenmail.store.FolderException;
import com.icegreen.greenmail.util.Retriever;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.talend.components.mail.configuration.MailConnection;
import org.talend.components.mail.configuration.RecipientTypeValue;
import org.talend.components.mail.greenmail.GreenMailExtension;
import org.talend.components.mail.service.MailService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.output.Processor;

@TestInstance(PER_CLASS)
@WithComponents("org.talend.components.mail")
class MailSenderTest {

    @RegisterExtension
    static GreenMailExtension SERVER = new GreenMailExtension()
            .withConfiguration(new GreenMailConfiguration().withUser("foo@bar.com", "foo@bar.com", "secret4Test"));

    @Injected
    private ComponentsHandler handler;

    @Service
    private RecordBuilderFactory records;

    @Service
    private MailService service;

    @AfterEach
    void purge() throws FolderException {
        SERVER.purgeEmailFromAllMailboxes();
    }

    @Test
    void smtpHeahthCheck() {
        final MailConnection connection = createConfiguration().getDataset().getConnection();
        assertEquals(HealthCheckStatus.Status.OK, service.check(connection).getStatus());
    }

    @Test
    void smtpHeahthCheckKo() {
        final MailConnection connection = createConfiguration().getDataset().getConnection();
        connection.setPort(7);
        assertEquals(HealthCheckStatus.Status.KO, service.check(connection).getStatus());
    }

    @Test
    void sendMail() throws IOException, MessagingException {
        final MailSender.Configuration configuration = createConfiguration();

        final Processor processor = handler.createProcessor(MailSender.class, configuration);
        final Record record = records.newRecordBuilder().withString("subject", "Hi from test")
                .withString("text", "Hello from record").build();
        handler.collect(processor, new JoinInputFactory().withInput("__default__", singletonList(record)));
        assertTrue(SERVER.waitForIncomingEmail(1));

        final Message[] messages = new Retriever(SERVER.getImap()).getMessages("foo@bar.com", "secret4Test");
        assertEquals(1, messages.length);

        final Message message = messages[0];
        assertEquals("Hi from test", message.getSubject());
        assertEquals("Hello from record", String.valueOf(message.getContent()).trim());
    }

    private MailSender.Configuration createConfiguration() {
        final MailSender.RecipientConfiguration defaultTo = new MailSender.RecipientConfiguration();
        defaultTo.setAddress("foo@bar.com");
        defaultTo.setType(RecipientTypeValue.TO);

        final MailSender.Configuration configuration = new MailSender.Configuration();
        final MailConnection connection = configuration.getDataset().getConnection();
        connection.setTransport(MailConnection.Transport.SMTP);
        connection.setUsername("foo@bar.com");
        connection.setPassword("secret4Test");
        connection.setHost("localhost");
        connection.setPort(SERVER.getSmtp().getPort());
        configuration.setDefaultFrom("MailSenderTest_sendMail@bar.com");
        configuration.setRecipients(singletonList(defaultTo));

        return configuration;
    }
}
