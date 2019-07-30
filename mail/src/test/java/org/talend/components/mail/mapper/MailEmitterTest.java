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
package org.talend.components.mail.mapper;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import com.icegreen.greenmail.server.AbstractServer;
import com.icegreen.greenmail.store.FolderException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.talend.components.mail.configuration.MailConnection;
import org.talend.components.mail.greenmail.GreenMailExtension;
import org.talend.components.mail.service.MailService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.junit.ComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.input.Mapper;

@TestInstance(PER_CLASS)
@WithComponents("org.talend.components.mail")
class MailEmitterTest {

    @RegisterExtension
    static GreenMailExtension SERVER = new GreenMailExtension()
            .withConfiguration(new GreenMailConfiguration().withUser("foo@bar.com", "foo@bar.com", "secret4Test"));

    @Injected
    private ComponentsHandler handler;

    @Service
    private MailService service;

    @AfterEach
    void purge() throws FolderException {
        SERVER.purgeEmailFromAllMailboxes();
    }

    @Test
    void pop3HeahthCheck() {
        final MailConnection connection = createConnection(MailConnection.Transport.POP3, SERVER.getPop3());
        assertEquals(HealthCheckStatus.Status.OK, service.check(connection).getStatus());
    }

    @Test
    void imapHeahthCheck() {
        final MailConnection connection = createConnection(MailConnection.Transport.IMAP, SERVER.getImap());
        assertEquals(HealthCheckStatus.Status.OK, service.check(connection).getStatus());
    }

    @Test
    void faillingCheck() {
        final MailConnection connection = createConnection(MailConnection.Transport.IMAP, SERVER.getImap());
        connection.setPort(7);
        assertEquals(HealthCheckStatus.Status.KO, service.check(connection).getStatus());
    }

    @Test
    void pop3() throws MessagingException, FolderException {
        final MailEmitter.SearchTermConfiguration term = new MailEmitter.SearchTermConfiguration();
        term.setType(MailEmitter.SearchType.SUBJECT);
        term.setPattern("Hello ");
        doTest(singletonList(term), MailConnection.Transport.POP3, SERVER.getPop3(), () -> term.setPattern("Hello "), 1);
    }

    @Test
    void imap() throws MessagingException, FolderException {
        final MailEmitter.SearchTermConfiguration seen = new MailEmitter.SearchTermConfiguration();
        seen.setType(MailEmitter.SearchType.FLAG);
        seen.getFlag().setFlag(MailEmitter.MailFlag.SEEN);
        seen.getFlag().setFlagValue(false);

        final MailEmitter.SearchTermConfiguration subject = new MailEmitter.SearchTermConfiguration();
        subject.setType(MailEmitter.SearchType.SUBJECT);
        subject.setPattern("Hello ");

        final List<MailEmitter.SearchTermConfiguration> terms = Stream.of(seen, subject).collect(toList());
        doTest(terms, MailConnection.Transport.IMAP, SERVER.getImap(), () -> terms.remove(1), 2);
    }

    private void doTest(final Collection<MailEmitter.SearchTermConfiguration> term, final MailConnection.Transport transport,
            final AbstractServer server, final Runnable changeTerm, final int expectedAfterChange)
            throws MessagingException, FolderException {
        final MailEmitter.Configuration configuration = new MailEmitter.Configuration();
        final MailConnection connection = createConnection(transport, server);
        configuration.getDataset().setConnection(connection);
        configuration.setTerms(term);

        sendMail("foo@bar.com", "dummy@toto.com", "Hello 1 from test", "First hi from test", connection);
        sendMail("foo@bar.com", "dummy@toto.com", "Hello 2 from test", "Second hello from test", connection);
        assertTrue(SERVER.waitForIncomingEmail(2));

        {
            final Mapper mapper = handler.createMapper(MailEmitter.class, configuration);
            final List<Record> mails = handler.collect(Record.class, mapper, 100).collect(toList());
            assertEquals(2, mails.size(), mails::toString);

            final Record record = mails.iterator().next();
            assertEquals("foo@bar.com", record.getString("to"));
            assertEquals("dummy@toto.com", record.getString("from"));
            assertEquals("Hello 1 from test", record.getString("subject"));
            assertEquals("First hi from test", record.getString("content").trim());
            assertEquals(10, record.getArray(Record.class, "headers").size());
        }

        SERVER.purgeEmailFromAllMailboxes();
        sendMail("foo@bar.com", "dummy@toto.com", "Hi 3 from test", "3rd hi from test", connection);
        assertTrue(SERVER.waitForIncomingEmail(1));
        assertEquals(0, handler.collect(Record.class, handler.createMapper(MailEmitter.class, configuration), 100).count());

        changeTerm.run();
        sendMail("foo@bar.com", "dummy@toto.com", "Hello 3 from test", "3rd hi from test", connection);
        assertTrue(SERVER.waitForIncomingEmail(1));
        configuration.setSkipHeaders(true);
        {
            final Mapper mapper = handler.createMapper(MailEmitter.class, configuration);
            final List<Record> records = handler.collect(Record.class, mapper, 100).collect(toList());
            assertEquals(expectedAfterChange, records.size(), records::toString);
            assertFalse(records.iterator().next().getOptionalArray(Record.class, "headers").isPresent());
        }
    }

    private MailConnection createConnection(final MailConnection.Transport transport, final AbstractServer server) {
        final MailConnection connection = new MailConnection();
        connection.setTransport(transport);
        connection.setUsername("foo@bar.com");
        connection.setPassword("secret4Test");
        connection.setHost("localhost");
        connection.setPort(server.getPort());
        return connection;
    }

    private void sendMail(final String to, final String from, final String subject, final String content,
            final MailConnection where) throws MessagingException {
        final MailConnection.Transport transport = where.getTransport();
        final int port = where.getPort();
        where.setTransport(MailConnection.Transport.SMTP);
        where.setPort(SERVER.getSmtp().getPort());
        final Session session = service.createSession(where);
        where.setTransport(transport);
        where.setPort(port);

        final MimeMessage message = new MimeMessage(session);
        message.setSubject(subject);
        message.setSentDate(new Date());
        message.setFrom(new InternetAddress(from));
        message.setRecipients(Message.RecipientType.TO, to);
        message.setText(content);

        Transport.send(message);
    }
}
