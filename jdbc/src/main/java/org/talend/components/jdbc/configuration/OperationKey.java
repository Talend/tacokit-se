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
package org.talend.components.jdbc.configuration;

import lombok.Data;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.talend.components.jdbc.service.UIActionService.ACTION_SUGGESTION_TABLE_COLUMNS_NAMES;
import static org.talend.sdk.component.api.configuration.condition.ActiveIfs.Operator.OR;

@Data
@GridLayout(value = { @GridLayout.Row({ "keys" }) })
@Documentation("Operation Key for output")
public class OperationKey implements Serializable {

    @Option
    @ActiveIfs(operator = OR, value = {
            @ActiveIf(target = "../../dataset.connection.dbType", value = { "SQLDWH" }, negate = true),
            @ActiveIf(target = "../../actionOnData", value = { "BULK_LOAD" }, negate = true) })
    @Suggestable(value = ACTION_SUGGESTION_TABLE_COLUMNS_NAMES, parameters = { "../../dataset" })
    @Documentation("List of columns to be used as keys for this operation")
    private List<String> keys = new ArrayList<>();

}
