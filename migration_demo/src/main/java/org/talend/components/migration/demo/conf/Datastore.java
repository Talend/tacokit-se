package org.talend.components.migration.demo.conf;

import lombok.Data;
import org.talend.components.migration.demo.handlers.DatastoreMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(value = Datastore.VERSION, migrationHandler = DatastoreMigrationHandler.class)
@GridLayout({@GridLayout.Row({"login"}), @GridLayout.Row({"password"})})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
@Data
@DataStore("Datastore")
public class Datastore implements Serializable {

    public final static int VERSION = 1;

    @Option
    @Documentation("")
    private String login;

    @Option
    @Documentation("")
    private String password;

}
