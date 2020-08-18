package org.talend.components.migration.demo.conf;

import lombok.Data;
import org.talend.components.migration.demo.handlers.DatasetMigrationHandler;
import org.talend.components.migration.demo.handlers.DatastoreMigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(value = Datastore.VERSION, migrationHandler = DatasetMigrationHandler.class)
@GridLayout({@GridLayout.Row({"date_format"}), @GridLayout.Row({"table"}), @GridLayout.Row({"columns"}), @GridLayout.Row({"dso"})})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = {})
@Data
@DataSet("Dataset")
public class Dataset implements Serializable {

    @Option
    @Documentation("")
    private String date_format;

    @Option
    @Documentation("")
    private String table;

    @Option
    @Documentation("")
    private String columns;

    @Option
    @Documentation("")
    private Datastore dso;

}
