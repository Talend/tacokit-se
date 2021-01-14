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
package org.talend.components.workday.dataset;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Code;
import org.talend.sdk.component.api.configuration.ui.widget.TextArea;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout(@GridLayout.Row("query"))
@Documentation("WQL layout for workday (Workday Query Language)")
public class WQLLayout implements Serializable, QueryHelper {

    private static final long serialVersionUID = 898158661235915308L;

    @Option
    @TextArea
    @Code("sql")
    @Documentation("A valid read only query is the source type is Query")
    private String query;

    @Override
    public String getServiceToCall() {
        return "wql/v1/data";
    }

    @Override
    public Map<String, String> extractQueryParam() {
        final String encodedQuery = this.encodeString(this.query);
        return Collections.singletonMap("query", encodedQuery);
    }

}
