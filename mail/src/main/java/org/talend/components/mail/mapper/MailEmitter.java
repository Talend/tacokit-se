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

import static java.util.Arrays.asList;
import static java.util.Collections.list;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static javax.mail.Folder.READ_ONLY;
import static javax.mail.Folder.READ_WRITE;
import static lombok.AccessLevel.NONE;
import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;
import javax.mail.Flags;
import javax.mail.Folder;
import javax.mail.Header;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.search.AndTerm;
import javax.mail.search.BodyTerm;
import javax.mail.search.ComparisonTerm;
import javax.mail.search.FlagTerm;
import javax.mail.search.FromTerm;
import javax.mail.search.HeaderTerm;
import javax.mail.search.NotTerm;
import javax.mail.search.OrTerm;
import javax.mail.search.ReceivedDateTerm;
import javax.mail.search.RecipientTerm;
import javax.mail.search.SearchTerm;
import javax.mail.search.SentDateTerm;
import javax.mail.search.SubjectTerm;

import org.talend.components.mail.configuration.MailConnection;
import org.talend.components.mail.configuration.MailDataSet;
import org.talend.components.mail.configuration.RecipientTypeValue;
import org.talend.components.mail.service.MailService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout.Row;
import org.talend.sdk.component.api.configuration.ui.widget.DateTime;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@Slf4j
@Version
@RequiredArgsConstructor
@Icon(value = CUSTOM, custom = "input")
@Emitter(name = "MailEmitter")
@Documentation("Component able to read a mailbox through a mail search query.")
public class MailEmitter implements Serializable {

    private static final long serialVersionUID = 1;

    private final Configuration configuration;

    @SuppressFBWarnings(justification = "services are serializable (talend component kit)", value = "SE_BAD_FIELD")
    private final MailService service;

    @SuppressFBWarnings(justification = "services are serializable (talend component kit)", value = "SE_BAD_FIELD")
    private final RecordBuilderFactory recordBuilderFactory;

    private transient State state;

    @Producer
    public Record next() {
        if (state == null) {
            switch (configuration.getDataset().getConnection().getTransport()) {
            case SMTP:
            case SMTPS:
                throw new IllegalArgumentException("SMTP transport is not supported to read mails");
            default:
            }
            try {
                state = createState();
            } catch (final MessagingException e) {
                onError(e);
            }
        }

        if (!state.messageIterator.hasNext()) {
            return null;
        }

        final Message message = state.messageIterator.next();
        final Record record = mapMessage(message);
        postMessageRead(message);
        return record;
    }

    @PreDestroy
    public void destroy() {
        if (state != null && state.messageIterator.hasNext()) {
            try {
                state.close();
            } catch (final MessagingException e) {
                onError(e);
            }
        }
    }

    private void postMessageRead(final Message message) {
        if (configuration.getFlagToSetOnceProduced() != null) {
            configuration.getFlagToSetOnceProduced().forEach(flag -> {
                try {
                    message.setFlags(flag.build(), true);
                } catch (final MessagingException e) {
                    onError(e);
                }
            });
        }
    }

    private SearchTerm createSearchTerm() {
        final SearchTerm trueTerm = new SearchTerm() {

            @Override
            public boolean match(final Message msg) {
                return true;
            }
        };
        return ofNullable(configuration.getTerms())
                .map(it -> it.stream().reduce(new SearchTermBuilder(null, trueTerm), (builder, config) -> {
                    SearchTerm newTerm = config.type.build(config);
                    if (config.isNegate()) {
                        newTerm = new NotTerm(newTerm);
                    }
                    return mergeTerms(builder, config, newTerm);
                }, (builder1, builder2) -> mergeTerms(builder1, builder2.configuration, builder2.term))).map(b -> b.term)
                .orElse(trueTerm);
    }

    private Schema.Entry createHeaderEntry() {
        return recordBuilderFactory.newEntryBuilder().withName("headers").withType(Schema.Type.ARRAY)
                .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                        .withEntry(recordBuilderFactory.newEntryBuilder().withName("key").withType(Schema.Type.STRING).build())
                        .withEntry(recordBuilderFactory.newEntryBuilder().withName("value").withType(Schema.Type.STRING).build())
                        .build())
                .build();
    }

    private State createState() throws MessagingException {
        final MailConnection connection = configuration.getDataset().getConnection();
        final Session session = service.createSession(connection);
        session.setDebug(configuration.isDebug());
        final Store store = session.getStore();
        store.connect(connection.getHost(), connection.getPort(), connection.getUsername(), connection.getPassword());
        final Folder folder = ofNullable(configuration.getInbox()).filter(it -> !it.isEmpty()).map(it -> {
            try {
                return store.getFolder(it);
            } catch (final MessagingException e) {
                return onError(e);
            }
        }).orElseGet(() -> {
            try {
                return store.getDefaultFolder();
            } catch (final MessagingException e) {
                return onError(e);
            }
        });
        folder.open(isReadOnly() ? READ_ONLY : READ_WRITE);
        final Message[] messages = folder.search(createSearchTerm());
        final Iterator<Message> messageIterator = asList(messages).iterator();
        return new State(folder, messageIterator, store, createHeaderEntry());
    }

    private boolean isReadOnly() {
        return configuration.getDataset().getConnection().getTransport().name().startsWith("POP3")
                || configuration.getFlagToSetOnceProduced() == null || configuration.getFlagToSetOnceProduced().isEmpty();
    }

    private Record mapMessage(final Message message) {
        try {
            final Record.Builder builder = recordBuilderFactory.newRecordBuilder()
                    .withString("to", message.getAllRecipients()[0].toString())
                    .withString("from", message.getFrom()[0].toString()).withString("subject", message.getSubject())
                    .withString("content", String.valueOf(message.getContent()));
            if (!configuration.isSkipHeaders()) {
                builder.withArray(state.headerEntry, mapHeaders(message));
            }
            return builder.build();
        } catch (final MessagingException | IOException e) {
            return onError(e);
        }
    }

    private Collection<?> mapHeaders(final Message message) throws MessagingException {
        return list((Enumeration<Header>) message.getAllHeaders()).stream().map(it -> recordBuilderFactory.newRecordBuilder()
                .withString("key", it.getName()).withString("value", it.getValue()).build()).collect(toList());
    }

    private SearchTermBuilder mergeTerms(final SearchTermBuilder builder1, final SearchTermConfiguration configuration,
            final SearchTerm term) {
        if (builder1 == null || builder1.configuration == null /* true term so replace it with real term */) {
            return new SearchTermBuilder(configuration, term);
        }
        switch (builder1.configuration.getOperator()) {
        case AND:
            return new SearchTermBuilder(configuration, new AndTerm(builder1.term, term));
        case OR:
            return new SearchTermBuilder(configuration, new OrTerm(builder1.term, term));
        default:
            throw new IllegalArgumentException(
                    "unsupported operator: " + builder1.configuration.operator + ", in " + builder1.configuration);
        }
    }

    private <T> T onError(final Exception e) {
        log.error(e.getMessage(), e);
        throw new IllegalStateException(e);
    }

    @Data
    @GridLayout({ @Row("dataset"), @Row("inbox"), @Row("terms") })
    @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @Row("debug"), @Row("skipHeaders"),
            @Row("flagToSetOnceProduced") })
    public static class Configuration implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("The mail dataset.")
        private MailDataSet dataset = new MailDataSet();

        @Option
        // @Required // cloud rules
        @Suggestable(value = "ListFolders", parameters = { "dataset", "." })
        @Documentation("The folder to read.")
        private String inbox = "INBOX";

        @Option
        @Documentation("Should headers be propagated in output records or not.")
        private boolean skipHeaders;

        @Option
        @Documentation("Should the session be set up in debug mode.")
        private boolean debug;

        @Option
        @Documentation("The filter to select the messages to read.")
        private Collection<SearchTermConfiguration> terms;

        @Option
        @Documentation("A list of flag to set on the message once read.")
        private Collection<FlagConfiguration> flagToSetOnceProduced;
    }

    @Data
    @GridLayout(@Row({ "flag", "customFlag", "flagValue" }))
    public static class FlagConfiguration implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("The flag to set, if set to `CUSTOM` it will use `customFlag` value.")
        private MailFlag flag = MailFlag.SEEN;

        @Option
        @ActiveIf(target = "flag", value = "CUSTOM")
        @Documentation("The custom flag value if `flag` is `CUSTOM`.")
        private String customFlag = "X-My-Flag";

        @Option
        @Documentation("The flag value (`true` or `false`).")
        private boolean flagValue;

        @Setter(NONE)
        @Getter(NONE)
        private transient Flags built;

        public synchronized Flags build() {
            return built == null ? built = ofNullable(flag).filter(it -> it != MailFlag.CUSTOM).map(MailFlag::getMailFlag)
                    .map(Flags::new).orElseGet(() -> new Flags(customFlag)) : built;
        }
    }

    @Data
    @GridLayout({ @Row({ "type", "negate", "operator" }), @Row({ "comparisonType", "absoluteDate" }),
            @Row({ "date", "relativeDate" }), @Row({ "recipientType", "address" }), @Row("flag"), @Row({ "header", "pattern" }) })
    public static class SearchTermConfiguration implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("Type of search term to create.")
        private SearchType type = SearchType.FLAG;

        @Option
        @Documentation("Should the term be negated.")
        private boolean negate;

        @Option
        @Documentation("How should this term be composed with next one (if any).")
        private OperatorType operator = OperatorType.AND;

        @Option
        @Documentation("Should the date be absolute or relative to `now`.")
        @ActiveIf(target = "type", value = { "RECEIVED_DATE", "SENT_DATE" })
        private boolean absoluteDate = false;

        @Option
        @Documentation("Relative date (from now) to use in the query.")
        @ActiveIf(target = "type", value = { "RECEIVED_DATE", "SENT_DATE" })
        @ActiveIf(target = "absoluteDate", value = "false")
        private RelativeDate relativeDate = new RelativeDate();

        @Option
        @DateTime
        @Documentation("Date to use in the query.")
        @ActiveIf(target = "type", value = { "RECEIVED_DATE", "SENT_DATE" })
        @ActiveIf(target = "absoluteDate", value = "true")
        private ZonedDateTime date = ZonedDateTime.now();

        @Option
        @Documentation("Comparison operator.")
        @ActiveIf(target = "type", value = { "RECEIVED_DATE", "SENT_DATE" })
        private ComparisonType comparisonType = ComparisonType.EQUALS;

        @Option
        @Documentation("Address to use to filter the messages in the selected folder.")
        @ActiveIf(target = "type", value = { "FROM", "TO" })
        private String address = "recipient@mail.com";

        @Option
        @Documentation("Recipient type.")
        @ActiveIf(target = "type", value = "TO")
        private RecipientTypeValue recipientType = RecipientTypeValue.TO;

        @Option
        @Documentation("Flag to filter on.")
        @ActiveIf(target = "type", value = "FLAG")
        private FlagConfiguration flag = new FlagConfiguration();

        @Option
        @Documentation("Header name.")
        @ActiveIf(target = "type", value = "HEADER")
        private String header = "My-Header";

        @Option
        @Documentation("Patter to filter on.")
        @ActiveIf(target = "type", value = { "BODY", "HEADER", "SUBJECT" })
        private String pattern = "My Pattern";

        private Date selectedDate() {
            return absoluteDate ? new Date(getDate().toInstant().toEpochMilli())
                    : new Date(System.currentTimeMillis()
                            + relativeDate.getUnit().getNativeunit().toMillis(relativeDate.getValue()));
        }
    }

    @Data
    @GridLayout(@Row({ "value", "unit" }))
    public static class RelativeDate implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("Value of the relative date (respecting `unit`). This can be negative")
        private int value = -1;

        @Option
        @Documentation("Unit of the value")
        private TimeUnitConfig unit = TimeUnitConfig.DAYS;
    }

    public enum OperatorType {
        AND,
        OR
    }

    public enum SearchType {
        HEADER {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new HeaderTerm(configuration.getHeader(), configuration.getPattern());
            }
        },
        SUBJECT {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new SubjectTerm(configuration.getPattern());
            }
        },
        BODY {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new BodyTerm(configuration.getPattern());
            }
        },
        FLAG {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new FlagTerm(configuration.getFlag().build(), configuration.getFlag().isFlagValue());
            }
        },
        RECEIVED_DATE {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new ReceivedDateTerm(configuration.getComparisonType().getComparison(), configuration.selectedDate());
            }
        },
        SENT_DATE {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                return new SentDateTerm(configuration.getComparisonType().getComparison(), configuration.selectedDate());
            }
        },
        FROM {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                try {
                    return new FromTerm(new InternetAddress(configuration.getAddress()));
                } catch (final AddressException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        },
        RECIPIENT {

            @Override
            public SearchTerm build(final SearchTermConfiguration configuration) {
                try {
                    final InternetAddress address = new InternetAddress(configuration.getAddress());
                    return new RecipientTerm(configuration.getRecipientType().getNativeType(), address);
                } catch (final AddressException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };

        public abstract SearchTerm build(final SearchTermConfiguration configuration);
    }

    @Getter
    @RequiredArgsConstructor
    public enum ComparisonType {
        EQUALS(ComparisonTerm.EQ),
        GREATER_OR_EQUAL(ComparisonTerm.GE),
        GREATER_THAN(ComparisonTerm.GT),
        LESS_OR_EQUAL(ComparisonTerm.LE),
        LESS_THAN(ComparisonTerm.LT),
        NOT_EQUAL(ComparisonTerm.NE);

        private final int comparison;
    }

    @Getter
    @RequiredArgsConstructor
    public enum MailFlag {
        SEEN(Flags.Flag.SEEN),
        FLAGGED(Flags.Flag.FLAGGED),
        ANSWERED(Flags.Flag.ANSWERED),
        CUSTOM(Flags.Flag.USER);

        @SuppressFBWarnings(justification = "enums are serializable", value = "SE_BAD_FIELD")
        private final Flags.Flag mailFlag;
    }

    @Getter
    @RequiredArgsConstructor
    public enum TimeUnitConfig {
        DAYS(TimeUnit.DAYS),
        HOURS(TimeUnit.HOURS),
        MINUTES(TimeUnit.MINUTES),
        SECONDS(TimeUnit.SECONDS),
        MILLISECONDS(TimeUnit.MILLISECONDS);

        private final TimeUnit nativeunit;
    }

    @RequiredArgsConstructor
    private static class SearchTermBuilder {

        private final SearchTermConfiguration configuration;

        private final SearchTerm term;
    }

    @RequiredArgsConstructor
    private static class State implements AutoCloseable {

        private final Folder folder;

        private final Iterator<Message> messageIterator;

        private final Store store;

        private final Schema.Entry headerEntry;

        @Override
        public void close() throws MessagingException {
            if (folder != null && folder.isOpen()) {
                folder.close(false);
            }
            if (store != null && store.isConnected()) {
                store.close();
            }
        }
    }
}
