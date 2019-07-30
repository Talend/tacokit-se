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

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.HeaderTokenizer;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;

import org.talend.components.mail.configuration.MailDataSet;
import org.talend.components.mail.configuration.RecipientTypeValue;
import org.talend.components.mail.service.MailService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout.Row;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Slf4j
@Version
@RequiredArgsConstructor
@Processor(name = "MailSender")
@Icon(value = CUSTOM, custom = "output")
@Documentation("Component able to send mails. It supports an incoming record with the following fields:\n\n"
        + "- `from`: the sender mail, if not set it will be taken from the component configuration\n"
        + "- `to`/`tos`: the `TO` recipient mail. If `to` is not present, "
        + "`tos` can be an array of string representing the target recipients mails\n"
        + "- `cc`/`ccs`: the `CC` (Carbon Copy) recipient mail. If `cc` is not present, "
        + "`ccs` can be an array of string representing the target recipients mails\n"
        + "- `bcc`/`bccs`: the `BCC` (Blind Carbon Copy) recipient mail. If `bcc` is not present, "
        + "`bccs` can be an array of string representing the target recipients mails\n"
        + "- `subject`: the mail subject, it is a required attribute\n" + "- `text`: the mail text, it is a required attribute\n"
        + "- `contentType`: the mail content type, default to `text/plain`\n"
        + "- `charset`: the mail content charset, default to `us-ascii`\n" + "- `description`: the mail description if any\n"
        + "- `headers`: an array of `Record` with a schema containing two string `key` and `value`. "
        + "It is then converted as mail headers\n\n")
public class MailSender implements Serializable {

    private static final long serialVersionUID = 1;

    private final Configuration configuration;

    @SuppressFBWarnings(justification = "services are serializable (talend component kit)", value = "SE_BAD_FIELD")
    private final MailService service;

    private transient State state;

    @ElementListener
    public void onElement(final Record record) {
        if (state == null) {
            doInit();
        }
        try { // note: if perf become an issue re-implement javax.mail.Transport.send0 to reuse the transport
            Transport.send(buildMessage(record));
        } catch (final MessagingException e) {
            throw new IllegalStateException(e);
        }
    }

    private void doInit() {
        switch (configuration.getDataset().getConnection().getTransport()) {
        case SMTP:
        case SMTPS:
            break;
        default:
            throw new IllegalArgumentException("Only SMTP transport is supported to send mails");
        }
        final Session session = service.createSession(configuration.getDataset().getConnection());
        session.setDebug(configuration.isDebug());
        try {
            state = new State(session, new InternetAddress(configuration.getDefaultFrom()),
                    ofNullable(configuration.getRecipients()).orElseGet(Collections::emptyList).stream()
                            .collect(groupingBy(conf -> conf.getType().getNativeType(), toList())).entrySet().stream()
                            .collect(toMap(Map.Entry::getKey, it -> it.getValue().stream().map(RecipientConfiguration::getAddress)
                                    .map(this::toInternetAddress).toArray(InternetAddress[]::new))));
        } catch (final AddressException e) {
            onError(e);
        }
    }

    private Message buildMessage(final Record record) throws MessagingException {
        final MimeMessage message = new MimeMessage(state.session);

        message.setFrom(configuration.isSupportConfigurationOverrideFromRecord()
                ? record.getOptionalString("from").map(this::toInternetAddress).orElse(state.from)
                : state.from);

        if (configuration.isSupportConfigurationOverrideFromRecord()) {
            setAddresses(record, message, "to", Message.RecipientType.TO);
            setAddresses(record, message, "cc", Message.RecipientType.CC);
            setAddresses(record, message, "bcc", Message.RecipientType.BCC);
        } else {
            state.getRecipients().forEach((k, v) -> {
                try {
                    message.addRecipients(k, v);
                } catch (final MessagingException e) {
                    throw new IllegalArgumentException(e);
                }
            });
        }

        record.getOptionalArray(Record.class, "headers").ifPresent(headers -> headers.forEach(header -> {
            try {
                message.setHeader(header.getString("key"), header.getString("value"));
            } catch (final MessagingException e) {
                onError(e);
            }
        }));

        message.setSentDate(new Date());
        message.setSubject(record.getString("subject"));
        record.getOptionalString("description").ifPresent(des -> {
            try {
                message.setDescription(des);
            } catch (final MessagingException e) {
                onError(e);
            }
        });

        final String text = record.getString("text");
        final Optional<String> contentType = record.getOptionalString("contentType");
        final Optional<String> charset = record.getOptionalString("charset");
        if (!contentType.isPresent() && !charset.isPresent()) {
            message.setText(text);
        } else {
            message.setContent(text, contentType.orElse("text/plain") + "; charset="
                    + MimeUtility.quote(charset.orElse("us-ascii"), HeaderTokenizer.MIME));
        }

        return message;
    }

    private void setAddresses(final Record record, final MimeMessage message, final String name,
            final Message.RecipientType type) {
        record.getOptionalString(name).map(this::toInternetAddress).map(it -> new InternetAddress[] { it }).map(Optional::of)
                .orElseGet(() -> record.getOptionalArray(String.class, name + 's')
                        .map(it -> it.stream().map(this::toInternetAddress).toArray(InternetAddress[]::new)).map(Optional::of)
                        .orElseGet(() -> ofNullable(state.getRecipients().get(type))))
                .ifPresent(addresses -> {
                    try {
                        message.addRecipients(type, addresses);
                    } catch (final MessagingException e) {
                        throw new IllegalArgumentException(e);
                    }
                });
    }

    private InternetAddress toInternetAddress(final String it) {
        try {
            return new InternetAddress(it);
        } catch (final AddressException e) {
            return onError(e);
        }
    }

    private <T> T onError(final MessagingException e) {
        log.error(e.getMessage(), e);
        throw new IllegalArgumentException(e);
    }

    @Data
    @GridLayout({ @Row("dataset"), @Row("defaultFrom"), @Row("recipients") })
    @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @Row("debug"), @Row("supportConfigurationOverrideFromRecord") })
    public static class Configuration implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("Mail dataset.")
        private MailDataSet dataset = new MailDataSet();

        @Option
        @Documentation("Should the session be in debug mode.")
        private boolean debug;

        @Option
        @Documentation("From header to use if not set in the incoming record.")
        private String defaultFrom;

        @Min(1)
        @Option
        @Documentation("Recipients to use if not set in the record.")
        private Collection<RecipientConfiguration> recipients;

        @Option
        @Documentation("Should the component support to read `from` and `recipients` configuration from "
                + "the incoming record or just respect the configuration.")
        private boolean supportConfigurationOverrideFromRecord = true;
    }

    @Data
    @GridLayout(@Row({ "type", "address" }))
    public static class RecipientConfiguration implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("Recipient type.")
        private RecipientTypeValue type;

        @Option
        @Documentation("Recipient address.")
        private String address;
    }

    @Data
    private static class State {

        private final Session session;

        private final InternetAddress from;

        private final Map<Message.RecipientType, InternetAddress[]> recipients;
    }
}
