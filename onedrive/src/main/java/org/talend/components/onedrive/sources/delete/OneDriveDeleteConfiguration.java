package org.talend.components.onedrive.sources.delete;

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
@GridLayout({ @GridLayout.Row({ "dataSet" }), @GridLayout.Row({ "fields" }) })
@Documentation("'Delete component' configuration")
public class OneDriveDeleteConfiguration implements Serializable {

    @Option
    @Documentation("OneDrive dataset")
    private OneDriveDataSet dataSet;

    @Option
    @Structure(discoverSchema = ConfigurationHelper.DISCOVER_SCHEMA_DELETE_ID, type = Structure.Type.OUT)
    @Documentation("The schema of the component. Use 'Discover schema' button to fill it with sample data.")
    private List<String> fields = new ArrayList<>();

}