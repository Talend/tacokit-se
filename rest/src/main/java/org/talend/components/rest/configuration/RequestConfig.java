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
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }) })
// @GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }) })
public class RequestConfig implements Serializable {

    @Option
    @Documentation("Identification of the resource to access")
    private Dataset dataset;

    public Map<String, String> pathParams() {
        if (!getDataset().getHasPathParams()) {
            return Collections.emptyMap();
        }

        return dataset.getPathParams().stream().collect(toMap(Param::getKey, Param::getValue));
    }

    public Map<String, String> queryParams() {
        if (!getDataset().getHasQueryParams()) {
            return Collections.emptyMap();
        }

        return dataset.getQueryParams().stream().collect(toMap(Param::getKey, Param::getValue));
    }

    public Map<String, String> headers() {
        if (!getDataset().getHasHeaders()) {
            return Collections.emptyMap();
        }

        return Collections.unmodifiableMap(dataset.getHeaders().stream().collect(toMap(Param::getKey, Param::getValue)));
    }

}
