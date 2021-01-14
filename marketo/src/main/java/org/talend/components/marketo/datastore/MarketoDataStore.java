/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.marketo.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.service.UIActionService.HEALTH_CHECK;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_URL_PROPERTY;

@Data
@DataStore(MarketoDataStore.NAME)
@GridLayouts({ //
        @GridLayout({ //
                @GridLayout.Row({ "endpoint" }), //
                @GridLayout.Row({ "clientId", "clientSecret" }) //
        }) //
})
@Checkable(HEALTH_CHECK)
@Documentation(MarketoDataStore.NAME)
@ToString
public class MarketoDataStore implements Serializable {

    public static final String NAME = "MarketoDataStore";

    @Option
    @Required
    @Validable(VALIDATION_URL_PROPERTY)
    @Documentation("Marketo endpoint (host only, ie: https://123-ABC-456.mktorest.com)")
    private String endpoint;

    @Option
    @Required
    @Documentation("Marketo client id")
    private String clientId;

    @Option
    @Required
    @Credential
    @Documentation("Marketo client secret")
    private String clientSecret;

}
