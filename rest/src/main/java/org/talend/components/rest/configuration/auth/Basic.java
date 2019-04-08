package org.talend.components.rest.configuration.auth;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "username", "password" }) })
@Documentation("Basic authentication configuration")
public class Basic implements Authorization, Serializable {

    @Option
    @Documentation("Username for the basic authentication")
    private String username;

    @Option
    @Credential
    @Documentation("password for the basic authentication")
    private String password;

    @Override
    public String getAuthorizationHeader() {
        return "Basic " + Base64.getEncoder()
                .encodeToString((this.getUsername() + ":" + this.getPassword()).getBytes(StandardCharsets.UTF_8));
    }
}
