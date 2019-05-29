package org.talend.components.onedrive.common;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
// @AllArgsConstructor
@EqualsAndHashCode
@GridLayout({ @GridLayout.Row({ "authenticationLogin" }), @GridLayout.Row({ "authenticationPassword" }) })
@Documentation("'Login' authentication settings")
public class AuthenticationLoginPasswordConfiguration implements Serializable, AuthenticationConfiguration {

    @Option
    @Documentation("Authentication login for 'Login' authentication")
    @Validable("validateAuthenticationLogin")
    @Setter
    private String authenticationLogin = "";

    @Option
    @Credential
    @Documentation("Authentication password for 'Login' authentication")
    @Setter
    private String authenticationPassword = "";

}
