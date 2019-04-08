package org.talend.components.rest.configuration.auth.oauth2;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "grantType" }),
        @GridLayout.Row({ "passwordGrant" }), @GridLayout.Row({ "authMethod" }), })
public class Oauth2 implements Serializable {

    @Option
    @DefaultValue("Password")
    @Documentation("")
    private GrantType grantType;

    @Option
    @ActiveIf(target = "grantType", value = "Password")
    @Documentation("")
    private PasswordGrant passwordGrant;

    @Option
    @DefaultValue("Header")
    @Documentation("")
    private AuthMethod authMethod;

    public enum GrantType {
        Password
    }

    public enum AuthMethod {
        Url,
        Header
    }

}
