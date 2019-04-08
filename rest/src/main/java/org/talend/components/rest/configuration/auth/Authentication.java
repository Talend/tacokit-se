package org.talend.components.rest.configuration.auth;

import java.io.Serializable;

import org.talend.components.rest.configuration.auth.oauth2.Oauth2;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataStore
@GridLayout({ @GridLayout.Row({ "resource" }), @GridLayout.Row({ "type" }), @GridLayout.Row({ "basic" }),
        @GridLayout.Row({ "bearerToken" }), @GridLayout.Row({ "oauth2" }), })
@Documentation("Http authentication data store")
public class Authentication implements Serializable {

    @Option
    @Documentation("End of url to complete base url of the datastore")
    private String resource;

    @Option
    @Documentation("")
    private Authorization.AuthorizationType type = Authorization.AuthorizationType.NoAuth;

    @Option
    @ActiveIf(target = "type", value = "Basic")
    @Documentation("")
    private Basic basic;

    @Option
    @Credential
    @ActiveIf(target = "type", value = "Bearer")
    @Documentation("")
    private String bearerToken;

    @Option
    @ActiveIf(target = "type", value = "Oauth2")
    @Documentation("")
    private Oauth2 oauth2;

}
