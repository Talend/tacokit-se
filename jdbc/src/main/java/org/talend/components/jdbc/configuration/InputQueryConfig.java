package org.talend.components.jdbc.configuration;

import lombok.Data;
import lombok.experimental.Delegate;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import static org.talend.sdk.component.api.configuration.ui.layout.GridLayout.FormType.ADVANCED;

@Data
@GridLayout(value = { @GridLayout.Row({ "dataSet" }) })
@GridLayout(names = ADVANCED, value = { @GridLayout.Row("advancedCommonConfig") })
@Documentation("Query input configuration")
public class InputQueryConfig implements InputConfig {

    @Option
    @Documentation("SQL query dataset")
    private SqlQueryDataset dataSet;

    @Option
    @Delegate
    @Documentation("common input configuration")
    private InputAdvancedCommonConfig advancedCommonConfig = new InputAdvancedCommonConfig();

}
