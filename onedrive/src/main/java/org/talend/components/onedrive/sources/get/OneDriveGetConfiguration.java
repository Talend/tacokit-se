package org.talend.components.onedrive.sources.get;

import lombok.Data;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Structure;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "fields" }), @GridLayout.Row({ "storeFilesLocally" }),
        @GridLayout.Row({ "storeDirectory" }) })
@Documentation("'Get component' configuration")
public class OneDriveGetConfiguration implements Serializable {

    @Option
    @Documentation("OneDrive dataset")
    private OneDriveDataSet dataSet;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_LIST_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

    @Option
    @Documentation("store files to local file system")
    private boolean storeFilesLocally = false;

    @Option
    @ActiveIf(target = "storeFilesLocally", value = { "true" })
    @Documentation("The directory where files will be stored and folder structure will be created")
    private String storeDirectory = "";

}