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
package org.talend.components.mail.service;

import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

import javax.mail.MessagingException;

import com.icegreen.greenmail.configuration.GreenMailConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.talend.components.mail.configuration.MailConnection;
import org.talend.components.mail.configuration.MailDataSet;
import org.talend.components.mail.greenmail.GreenMailExtension;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.junit5.WithComponents;

@TestInstance(PER_CLASS)
@WithComponents("org.talend.components.mail")
class MailServiceTest {

    @RegisterExtension
    static GreenMailExtension SERVER = new GreenMailExtension()
            .withConfiguration(new GreenMailConfiguration().withUser("foo@bar.com", "foo@bar.com", "secret4Test"));

    @Service
    private MailService service;

    @Test
    void listFolders() throws MessagingException {
        final MailConnection connection = new MailConnection();
        connection.setTransport(MailConnection.Transport.IMAP);
        connection.setUsername("foo@bar.com");
        connection.setPassword("secret4Test");
        connection.setHost("localhost");
        connection.setPort(SERVER.getImap().getPort());

        final MailDataSet dataSet = new MailDataSet();
        dataSet.setConnection(connection);

        assertEquals("INBOX", list(dataSet, ""));
        assertEquals("INBOX", list(dataSet, "/"));
        assertEquals("INBOX", list(dataSet, "I"));
        assertEquals("INBOX", list(dataSet, "IN"));
        assertEquals("INBOX", list(dataSet, "INBO"));
        assertEquals("INBOX", list(dataSet, "INBOX"));
        assertEquals("INBOX", list(dataSet, "INBOXI")); // falls back to propose something real
    }

    private String list(final MailDataSet dataSet, final String folder) throws MessagingException {
        return service.listFolders(dataSet, folder).getItems().stream().map(SuggestionValues.Item::getId).collect(joining(","));
    }
}
