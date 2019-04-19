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
package org.talend.components.rest.configuration;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Data
@Documentation("")
@OptionsOrder({ "type", "offsetParameterType", "offsetParameterName", "offsetIncrement", "urlLocaltion", "headerParameterName",
        "urlJsonPointer" })
public class Pagination implements Serializable {

    public enum Strategy {
        LIMIT_OFFSET,
        NEXT_URL
    }

    public enum OffsetParameterLocation {
        HEADER,
        QUERY
    }

    public enum NextUrlLocation {
        HEADER,
        PAYLOAD
    }

    @Option
    @Documentation("")
    private Strategy type;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "LIMIT_OFFSET")
    private OffsetParameterLocation offsetParameterType;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "LIMIT_OFFSET")
    private String offsetParameterName;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "LIMIT_OFFSET")
    private Integer offsetIncrement;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "NEXT_URL")
    private NextUrlLocation urlLocaltion;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "NEXT_URL")
    @ActiveIf(target = "urlLocaltion", value = "HEADER")
    private String headerParameterName;

    @Option
    @Documentation("")
    @ActiveIf(target = "type", value = "NEXT_URL")
    @ActiveIf(target = "urlLocaltion", value = "PAYLOAD")
    private String urlJsonPointer;

}
