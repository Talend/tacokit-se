// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.dataset;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

@Data
@GridLayout({ //
        @GridLayout.Row({ "dataSet" }), //
}) //
@Documentation("Marketo Source Configuration")
@ToString(callSuper = true)
public class MarketoInputConfiguration implements Serializable {

    public static final String NAME = "MarketoInputConfiguration";

    /*
     * DataSet
     */
    @Option
    @Documentation("Marketo DataSet")
    private MarketoDataSet dataSet;

}
