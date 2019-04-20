package org.talend.components.solr.common;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataStore("SolrDataStore")
@Checkable("checkSolrConnection")
@Documentation("A connection to Solr server")
@GridLayout({ @GridLayout.Row({ "url" }), @GridLayout.Row({ "login" }), @GridLayout.Row({ "password" }) })
public class SolrDataStore implements Serializable {

    @Option
    @Required
    @Pattern("^(http://|https://).*")
    @Documentation("Solr server connection URL")
    private String url;

    @Option
    @Documentation("Solr server user")
    private String login;

    @Option
    @Credential
    @Documentation("Solr server password")
    private String password;

}
