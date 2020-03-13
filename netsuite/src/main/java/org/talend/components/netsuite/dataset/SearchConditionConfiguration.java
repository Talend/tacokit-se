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
package org.talend.components.netsuite.dataset;

import java.io.Serializable;

import org.talend.components.netsuite.service.UIActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIf.EvaluationStrategy;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Documentation("Search Condition parameters")
@Data
@OptionsOrder({ "field", "operator", "searchValue", "additionalSearchValue" })
@NoArgsConstructor
@AllArgsConstructor
public class SearchConditionConfiguration implements Serializable {

    @Option
    @Suggestable(value = UIActionService.LOAD_FIELDS, parameters = { "../../dataSet" })
    @Documentation("Field, that will be used for search")
    private String field;

    @Option
    @Suggestable(value = UIActionService.LOAD_OPERATORS, parameters = { "../../dataSet", "field" })
    @Documentation("Operator")
    private String operator;

    @Option
    @ActiveIf(target = "operator", negate = true, value = { "String.empty", "String.notEmpty", "Long.empty", "Long.notEmpty",
            "Double.empty", "Double.notEmpty", "Date.empty", "Date.notEmpty", "TextNumber.empty", "TextNumber.notEmpty",
            "PredefinedDate" }, evaluationStrategy = EvaluationStrategy.CONTAINS)
    @Documentation("Search Value")
    private String searchValue;

    @Option
    @ActiveIf(target = "operator", value = { "Long.between", "Long.notBetween", "Double.between", "Double.notBetween",
            "Date.within", "Date.notWithin", "TextNumber.between", "TextNumber.notBetween" })
    @Documentation("Additional search value used with operators like between, within or negate of them")
    private String additionalSearchValue;

}