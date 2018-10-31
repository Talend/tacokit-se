// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Max;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

@Data
@DataSet
@Documentation("Marketo Base Dataset")
@ToString
public class MarketoDataSet implements Serializable {

    public enum MarketoEntity {
        Lead,
        List,
        CustomObject,
        Company,
        Opportunity,
        OpportunityRole
    }

    @Option
    @Documentation("DataStore")
    private MarketoDataStore dataStore;

    @Option
    @DefaultValue(value = "Lead")
    @Documentation("Marketo Entity to manage")
    private MarketoEntity entity;

    @Option
    @Min(1)
    @Max(300)
    @DefaultValue(value = "200")
    @Documentation("Batch Size")
    private Integer batchSize;

}
