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
package org.talend.components.fileio.s3;

import lombok.Getter;
import lombok.Setter;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

/**
 * This class represents configuration that should be inherited from a @Datastore class, or composed into a @Datastore
 * class. It represents a piece of reusable S3DataStore configuration.
 *
 * At the time of writing, the main purpose of this configuration extraction, is the ability to reuse a @Datastore into
 * another configuration, as a reference, without considering it as a new configuration type from the component server
 * standpoint.
 */
@Getter
@Setter
public class S3DataStoreConfiguration implements Serializable {

    @Option
    @Documentation("Should this datastore be secured and use access/secret keys.")
    private boolean specifyCredentials = true;

    @Option
    // @Required // todo: ensure ui supports to bypass this validation if not visible
    @ActiveIf(target = "specifyCredentials", value = "true")
    @Documentation("The S3 access key")
    private String accessKey;

    @Option
    @Credential
    // @Required // todo: ensure ui supports to bypass this validation if not visible
    @ActiveIf(target = "specifyCredentials", value = "true")
    @Documentation("The S3 secret key")
    private String secretKey;

}
