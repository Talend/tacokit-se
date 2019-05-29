package org.talend.components.onedrive.sources.put;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "fields" }), @GridLayout.Row({ "dataSource" }) })
@Documentation("'Put component' configuration")
public class OneDrivePutConfiguration implements Serializable {

    @Option
    @Documentation("OneDrive dataset")
    private OneDriveDataSet dataSet;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    public enum DataSource {
        File,
        Content
    }

    @Option
    @Documentation("The source of data. File - if we want to put local files, Content - if we want to put byte array")
    private DataSource dataSource = DataSource.Content;
}