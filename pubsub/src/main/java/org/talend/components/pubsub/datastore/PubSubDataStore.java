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
package org.talend.components.pubsub.datastore;

import lombok.Data;

import java.io.Serializable;

import org.talend.components.pubsub.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@GridLayout({ //
        @GridLayout.Row({ "projectName" }), //
        @GridLayout.Row("serviceAccountFile") //
})
@DataStore("PubSubDataStore")
@Checkable(UIActionService.ACTION_HEALTH_CHECK)
@Documentation("TODO")
public class PubSubDataStore implements Serializable {

    @Option
    @Required
    @Documentation("TODO")
    private String projectName;

    @Option
    @Required
    @Documentation("TODO")
    private String serviceAccountFile;

}
