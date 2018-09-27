package org.talend.components.jdbc.datastore;

import java.io.Serializable;

import org.talend.components.jdbc.service.ActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("dbType"), @GridLayout.Row("jdbcUrl"), @GridLayout.Row("userId"), @GridLayout.Row("password"), })
@DataStore("basic")
@Checkable(ActionService.ACTION_BASIC_HEALTH_CHECK)
@Documentation("A connection to a data base")
public class BasicDatastore implements Serializable {

    @Option
    @Required
    @Documentation("Data base type from the supported data base list")
    @Proposable(ActionService.ACTION_LIST_SUPPORTED_DB)
    private String dbType;

    @Option
    @Required
    @Documentation("jdbc connection url")
    private String jdbcUrl;

    @Option
    @Documentation("database user")
    private String userId;

    @Option
    @Credential
    @Documentation("database password")
    private String password;

}
