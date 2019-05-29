package org.talend.components.onedrive.sources.create;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "fields" }), @GridLayout.Row({ "createDirectoriesByList" }),
        @GridLayout.Row({ "objectType" }), @GridLayout.Row({ "objectPath" }) })
@Documentation("'Create component' configuration")
public class OneDriveCreateConfiguration implements Serializable {

    @Option
    @Documentation("OneDrive dataset")
    private OneDriveDataSet dataSet;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @Documentation("The option to create directories using list of paths")
    private boolean createDirectoriesByList;

    @Option
    @ActiveIf(target = "createDirectoriesByList", value = { "false" })
    @Documentation("The name of file or folder to create. Use '/' as a directory delimiter")
    private String objectPath = "";

    @Option
    @ActiveIf(target = "createDirectoriesByList", value = { "false" })
    @Documentation("The type of created object")
    private OneDriveObjectType objectType = OneDriveObjectType.DIRECTORY;

}