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
package org.talend.components.azure.service;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.Comparison;
import org.talend.components.azure.table.input.InputProperties;

import com.microsoft.azure.storage.OperationContext;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AzureTableUtilsTest {

    @Test
    public void testOpContextCreatedForFirstTime() {
        OperationContext context = AzureConnectionService.getTalendOperationContext();

        assertNotNull(context);
        assertFalse(context.getUserHeaders().isEmpty());
        assertNotNull(context.getUserHeaders().get(AzureConnectionService.USER_AGENT_KEY));
    }

    @Test
    public void testOpContextIsSingleTone() {
        OperationContext contextFirst = AzureConnectionService.getTalendOperationContext();

        assertEquals(contextFirst, AzureConnectionService.getTalendOperationContext());
    }

    @Test
    public void testGenerateFilterExpression() {
        String columnName = "someColumn";
        InputProperties configuration = new InputProperties();
        InputProperties.FilterExpression expression1 = new InputProperties.FilterExpression();
        expression1.setColumn(columnName);
        expression1.setFieldType(InputProperties.FieldType.NUMERIC);
        expression1.setFunction(Comparison.GREATER_THAN);
        expression1.setValue("2");
        List<InputProperties.FilterExpression> filterExpressions = new ArrayList<>();
        filterExpressions.add(expression1);
        configuration.setFilterExpressions(filterExpressions);

        String resultFilterExpression = AzureTableUtils.generateCombinedFilterConditions(configuration);

        assertFalse(resultFilterExpression.isEmpty());
        assertTrue(resultFilterExpression.contains(columnName));
        assertTrue(resultFilterExpression.contains("gt"));
        assertTrue(resultFilterExpression.contains("2"));
    }
}