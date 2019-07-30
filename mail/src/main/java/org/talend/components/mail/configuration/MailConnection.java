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
package org.talend.components.mail.configuration;

import java.io.Serializable;
import java.util.Collection;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout.Row;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@Data
@ToString(exclude = { "password" })
@DataStore("MailConnection")
@GridLayout({ @Row("transport"), @Row({ "host", "port" }), @Row({ "tls", "auth" }), @Row({ "username", "password" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @Row("properties"), @Row("timeout") })
@Checkable("MailConnectionCheck")
@Documentation("A connection to a data base")
public class MailConnection implements Serializable {

    private static final long serialVersionUID = 1;

    @Option
    @Documentation("Transport for the current connection, "
            + "it will likely be POP3(s), IMAP(s) or STMP(s) depending your mail provider.")
    private Transport transport = Transport.IMAP;

    @Option
    @Required
    @Documentation("The mail server host.")
    private String host;

    @Option
    @Documentation("The mail server port.")
    private int port;

    @Option
    @Documentation("The timeout for that connection, it is propagated for connect/read/write timeouts.")
    private int timeout;

    @Option
    @Documentation("Should TLS be actived.")
    private boolean tls;

    @Option
    @Documentation("Is authentication active.")
    private boolean auth;

    @Option
    @ActiveIf(target = "auth", value = "true")
    @Documentation("Username for authentication.")
    private String username;

    @Option
    @Credential
    @ActiveIf(target = "auth", value = "true")
    @Documentation("Password for authentication.")
    private String password;

    @Option
    @Documentation("Other `Session` properties, "
            + "see https://javaee.github.io/javamail/docs/api/com/sun/mail/smtp/package-summary.html for details.")
    private Collection<Property> properties;

    @Data
    public static class Property implements Serializable {

        private static final long serialVersionUID = 1;

        @Option
        @Documentation("Property name, generally it has the form of `mail.xxx.yyy`.")
        private String name;

        @Option
        @Documentation("The property value.")
        private String value;
    }

    @Getter
    @RequiredArgsConstructor
    public enum Transport {
        SMTP("smtp"),
        SMTPS("smtps"),
        NNTP_POST("nntp-post"),
        NNTP_POSTS("nntp-posts"),
        POP3("pop3"),
        POP3S("pop3s"),
        IMAP("imap"),
        IMAPS("imaps");

        private final String protocol;
    }
}
