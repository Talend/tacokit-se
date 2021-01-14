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
package org.talend.components.dynamicscrm.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.dynamicscrm.service.I18n;
import org.talend.components.dynamicscrm.source.DynamicsCrmInputMapperConfiguration.Operator;
import org.talend.components.dynamicscrm.source.FilterCondition.FilterOperator;
import org.talend.components.dynamicscrm.source.OrderByCondition.Order;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.dynamicscrm")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class InputHelperTest {

    @Service
    private I18n i18n;

    private InputHelper helper;

    @BeforeAll
    public void initHelper() {
        helper = new InputHelper(i18n);
    }

    @Test
    public void testEmptyFilterConversion() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNull(filterQuery);
    }

    @Test
    public void testOneFieldEquals() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field eq 'value')", filterQuery);
    }

    @Test
    public void testOneFieldNotEquals() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field ne 'value')", filterQuery);
    }

    @Test
    public void testOneFieldGreater() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field gt '5')", filterQuery);
    }

    @Test
    public void testOneFieldGreaterOrEqual() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field ge '5')", filterQuery);
    }

    @Test
    public void testOneFieldLess() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field lt '5')", filterQuery);
    }

    @Test
    public void testOneFieldLessOrEqual() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals("(field le '5')", filterQuery);
    }

    @Test
    public void testMultipleAnd() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals(
                "(field eq 'value') and (field ne 'value') and (field gt '5') and (field ge '5') and (field lt '5') and (field le '5')",
                filterQuery);
    }

    @Test
    public void testMultipleOr() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setCustomFilter(false);
        configuration.setOperator(Operator.OR);
        List<FilterCondition> filterConditionList = new ArrayList<>();
        filterConditionList.add(new FilterCondition("field", FilterOperator.EQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.NOTEQUAL, "value"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.GREATER_OR_EQUAL, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_THAN, "5"));
        filterConditionList.add(new FilterCondition("field", FilterOperator.LESS_OR_EQUAL, "5"));
        configuration.setFilterConditions(filterConditionList);

        String filterQuery = helper.getFilterQuery(configuration);

        assertNotNull(filterQuery);
        assertEquals(
                "(field eq 'value') or (field ne 'value') or (field gt '5') or (field ge '5') or (field lt '5') or (field le '5')",
                filterQuery);
    }

    @Test
    public void testOrderByFieldAsc() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("field", Order.ASC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field", orderQuery);
    }

    @Test
    public void testOrderByFieldDesc() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("field", Order.DESC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field desc", orderQuery);
    }

    @Test
    public void testOrderBySeveralFields() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(
                Arrays.asList(new OrderByCondition("field", Order.DESC), new OrderByCondition("field1", Order.ASC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNotNull(orderQuery);
        assertEquals("field desc,field1", orderQuery);
    }

    @Test
    public void testEmptyFieldOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Arrays.asList(new OrderByCondition("", Order.DESC)));

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

    @Test
    public void testEmptyOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(Collections.emptyList());

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

    @Test
    public void testNullListOrderBy() {
        DynamicsCrmInputMapperConfiguration configuration = new DynamicsCrmInputMapperConfiguration();
        configuration.setOrderByConditionsList(null);

        String orderQuery = helper.getOrderByQuery(configuration);

        assertNull(orderQuery);
    }

}
