package org.talend.components.onedrive.common;

import lombok.Data;
import org.talend.components.onedrive.helpers.ConfigurationHelper;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@DataSet(ConfigurationHelper.DATA_SET_ID)
@GridLayout({ @GridLayout.Row({ "dataStore" }) })
@Documentation("Data set")
public class OneDriveDataSet implements Serializable {

    @Option
    @Documentation("Connection to server")
    private OneDriveDataStore dataStore;

}