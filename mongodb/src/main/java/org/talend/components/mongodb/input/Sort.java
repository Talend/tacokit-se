package org.talend.components.mongodb.input;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@GridLayout({ @GridLayout.Row({ "Column" }), @GridLayout.Row({ "Order" }) })
@Documentation("This is the mapping for input schema.")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Sort {

    @Option
    @Suggestable(value = "loadFields", parameters = { "../../commonDataSet" })
    @Documentation("TODO fill the documentation for this parameter")
    private String Column;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String Order;
}
