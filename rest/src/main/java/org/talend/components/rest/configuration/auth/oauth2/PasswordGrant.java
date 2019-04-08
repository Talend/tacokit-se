package org.talend.components.rest.configuration.auth.oauth2;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "accessTokeUrl" }), @GridLayout.Row({ "username" }),
        @GridLayout.Row({ "password" }), @GridLayout.Row({ "clientId" }), @GridLayout.Row({ "clientSecret" }),
        @GridLayout.Row({ "scopes" }), })
public class PasswordGrant {

    @Option
    @Pattern("^(http|https)://")
    @Documentation("")
    private final String accessTokeUrl;

    @Option
    @Documentation("")
    private final String username;

    @Option
    @Credential
    @Documentation("")
    private final String password;

    @Option
    @Documentation("")
    private final String clientId;

    @Option
    @Credential
    @Documentation("")
    private final String clientSecret;

    @Option
    @Documentation("")
    private String scopes;

}
