/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.rest.configuration;

import lombok.Data;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.components.rest.service.RestService;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(1)
@Data
@DataStore("Datastore")
@Checkable(RestService.HEALTHCHECK)
@Documentation("Define where is the REST API and its description.")
@GridLayout({ @GridLayout.Row({ "base" }), @GridLayout.Row({ "authentication" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "connectionTimeout" }),
        @GridLayout.Row({ "readTimeout" }) })
public class Datastore implements Serializable {

    @Option
    @Required
    @Pattern("^https?://.+$")
    @Documentation("")
    // @Suggestable(value = "getBase", parameters = { ".." })
    private String base;

    @Option
    @Required
    @Documentation("")
    private Authentication authentication;

    @Min(0)
    @Option
    @Required
    @Documentation("")
    @DefaultValue("30000")
    private Integer connectionTimeout = 30000;

    @Min(0)
    @Option
    @Required
    @Documentation("")
    @DefaultValue("120000")
    private Integer readTimeout = 120000;

}