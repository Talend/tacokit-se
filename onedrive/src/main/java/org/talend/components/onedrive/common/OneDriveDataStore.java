package org.talend.components.onedrive.common;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Getter
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@DataStore(ConfigurationHelper.DATA_STORE_ID)
@Checkable(ConfigurationHelper.DATA_STORE_HEALTH_CHECK)
@GridLayout({ @GridLayout.Row({ "tenantId" }), @GridLayout.Row({ "applicationId" }),
        @GridLayout.Row({ "authenticationLoginPasswordConfiguration" }) })
@Documentation("Data store settings. OneDrive's server connection and authentication preferences")
public class OneDriveDataStore implements Serializable {

    @Option
    @Documentation("Tenant ID is a globally unique identifier. That is used for configuring Windows group policy for OneDrive for Business")
    @Validable("validateTenantId")
    @Setter
    private String tenantId = "";

    @Option
    @Documentation("OneDrive Application ID")
    @Validable("validateApplicationId")
    @Setter
    private String applicationId = "";

    @Option
    @Documentation("authentication type (Login etc.)")
    @Setter
    private AuthenticationType authenticationType = AuthenticationType.LOGIN_PASSWORD;

    @Option
    @Documentation("authentication Login settings")
    @Setter
    private AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration;

    public AuthenticationConfiguration getAuthSettings() throws UnknownAuthenticationTypeException {
        if (authenticationType == AuthenticationType.LOGIN_PASSWORD) {
            return authenticationLoginPasswordConfiguration;
        }
        throw new UnknownAuthenticationTypeException();
    }
}