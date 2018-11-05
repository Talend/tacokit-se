package org.talend.components.zendesk.common;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.talend.components.zendesk.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
@DataStore(ConfigurationHelper.DATA_STORE_ID)
@Checkable(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "serverUrl" }), @GridLayout.Row({ "authenticationType" }),
        @GridLayout.Row({ "authenticationLoginPasswordConfiguration" }),
        @GridLayout.Row({ "authenticationApiTokenConfiguration" }) })
@Documentation("Data store settings. Zendesk's server connection and authentication preferences")
public class ZendeskDataStore implements Serializable {

    @Option
    @Documentation("Zendesk server URL")
    @Pattern("^(http://|https://).*")
    private String serverUrl = "";

    @Option
    @Documentation("authentication type (Login etc.)")
    private AuthenticationType authenticationType = AuthenticationType.LOGIN_PASSWORD;

    @Option
    @Documentation("authentication Login settings")
    @ActiveIf(target = "authenticationType", value = { "LOGIN_PASSWORD" })
    private AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration;

    @Option
    @Documentation("authentication API token settings")
    @ActiveIf(target = "authenticationType", value = { "API_TOKEN" })
    private AuthenticationApiTokenConfiguration authenticationApiTokenConfiguration;

}