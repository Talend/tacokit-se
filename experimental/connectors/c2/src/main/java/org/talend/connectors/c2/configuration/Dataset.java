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
package org.talend.connectors.c2.configuration;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@DataSet("Dataset")
@GridLayout({ @GridLayout.Row("datastore"), @GridLayout.Row("nbeRecord") })
@Documentation("Dataset configuration.")
public class Dataset implements Serializable {

    @Option
    @Documentation("Identification of the REST API.")
    private Datastore datastore;

    @Option
    @Documentation("How many redirection are supported ? (-1 for infinite)")
    @DefaultValue("3")
    private Integer nbeRecord = 3;

}
