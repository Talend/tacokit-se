package org.talend.components.jms.datastore;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

import static org.talend.components.jms.service.ActionService.ACTION_BASIC_HEALTH_CHECK;
import static org.talend.components.jms.service.ActionService.ACTION_LIST_SUPPORTED_BROKER;

@Data
@GridLayout({
        @GridLayout.Row({"moduleList"}),
        @GridLayout.Row("url"),
        @GridLayout.Row("userIdentity"),
        @GridLayout.Row({"userName", "password"})})
@DataStore
@Checkable(ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class JmsDataStore implements Serializable {

    @Option
    @Required
    @Documentation("Data type from the supported jms providers list")
    @Proposable(ACTION_LIST_SUPPORTED_BROKER)
    private String moduleList;

    @Option
    @Pattern("^(tcp)://")
    @Documentation("Input for JMS server URL")
    private String url = "tcp://host:port";

    @Option
    @Documentation("Checkbox for User login/password checking")
    private boolean userIdentity = false;

    @Option
    @Documentation("Input for User Name")
    @ActiveIf(target = "userIdentity", value = "true")
    private String userName;

    @Option
    @Credential
    @Documentation("Input for password")
    @ActiveIf(target = "userIdentity", value = "true")
    private String password;

}
