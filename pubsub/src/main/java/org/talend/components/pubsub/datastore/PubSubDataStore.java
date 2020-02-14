/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.pubsub.datastore;

import lombok.Data;
import org.talend.components.pubsub.service.PubSubService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@DataStore("PubSubDataStore")
@Data
@Icon(value = Icon.IconType.CUSTOM, custom = "pubsub")
@Checkable(PubSubService.ACTION_HEALTH_CHECK)
@GridLayout({ //
        @GridLayout.Row({ "projectName" }), //
        @GridLayout.Row("jsonCredentials") //
})
@Documentation("Pub/Sub DataStore Properties")
public class PubSubDataStore implements Serializable {

    @Option
    @Required
    @Documentation("Google Cloud Platform Project")
    private String projectName;

    @Option
    @Credential
    @Required
    @Documentation("Google credential (JSON)")
    private String jsonCredentials;

}
