package org.talend.components.salesforce.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataStore("basic")
@Checkable("basic.healthcheck")
@GridLayout({ @GridLayout.Row({ "userId" }), @GridLayout.Row({ "password", "securityKey" }) })
@Documentation("")
public class BasicDataStore implements Serializable {

    @Option
    @Required
    @Documentation("")
    public String userId;

    @Option
    @Required
    @Credential
    @Documentation("")
    public String password;

    @Option
    @Required
    @Credential
    @Documentation("")
    public String securityKey;

}
