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
package org.talend.components.google.storage.dataset;

import java.io.Serializable;

import org.talend.components.google.storage.datastore.GSDataStore;
import org.talend.components.google.storage.service.GSService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@DataSet("GoogleStorageDataSet")
@GridLayout({ @GridLayout.Row("dataStore"), @GridLayout.Row({ "bucket", "blob" }), @GridLayout.Row("contentFormat") })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("contentFormat") })
@Documentation("Dataset for google storage")
@Slf4j
public class GSDataSet implements Serializable {

    private static final long serialVersionUID = 6928259038341692558L;

    @Option
    @Documentation("The connector to google storage.")
    private GSDataStore dataStore;

    @Option
    @Required
    @Documentation("Bucket name of google storage.")
    @Suggestable(value = GSService.ACTION_SUGGESTION_BUCKET, parameters = { "dataStore" })
    private String bucket;

    @Option
    @Required
    @Documentation("Blob name (file) for bucket.")
    @Suggestable(value = GSService.ACTION_SUGGESTION_BLOB, parameters = { "dataStore", "bucket" })
    private String blob;

    @Option
    @Required
    @Documentation("Blob content format, CSV, Json ...")
    private FormatConfiguration contentFormat = new FormatConfiguration();

}
