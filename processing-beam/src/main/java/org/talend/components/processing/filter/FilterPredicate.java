// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.processing.filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.commons.lang3.StringUtils;

import org.talend.components.processing.ProcessingErrorCode;
import scala.collection.JavaConversions;
import scala.util.Try;
import wandou.avpath.Evaluator;

/**
 * A function that takes an input IndexedRecord and returns whether it matches the given filter criteria.
 */
public class FilterPredicate implements SerializableFunction<IndexedRecord, Boolean> {

    private final FilterConfiguration configuration;

    FilterPredicate(FilterConfiguration configuration) {
        this.configuration = configuration;
        // Remove any filter criteria that have not been initialized by the user. These elements are ignored.
        List<FilterConfiguration.Criteria> filters = this.configuration.getFilters();
        this.configuration.setFilters(new ArrayList<>());
        for (FilterConfiguration.Criteria criteria : filters) {
            if (!StringUtils.isEmpty(criteria.getColumnName()) || !StringUtils.isEmpty(criteria.getValue())) {
                this.configuration.getFilters().add(criteria);
            }
        }
    }

    @Override
    public Boolean apply(IndexedRecord input) {
        // Short-circuit when the user hasn't filled out any filters.
        if (configuration.getFilters().isEmpty())
            return true;

        // This is the logical operation applied to the set of filter criteria in this component.
        // (i.e. ALL means that all criteria must evaluate to true.)
        LogicalOpType criteriaLogicalOp = configuration.getLogicalOpType();

        // Starting point for aggregating the logical operations.
        boolean aggregate = criteriaLogicalOp.createAggregate();

        // Apply all of the criteria.
        for (FilterConfiguration.Criteria criteria : configuration.getFilters()) {
            aggregate = criteriaLogicalOp.combineAggregate(aggregate, evaluateCriteria(criteria, input));
            if (criteriaLogicalOp.canShortCircuit(aggregate))
                break;
        }

        return aggregate;
    }

    /**
     * Evaluate one specific criteria against the given indexed record.
     *
     * @param criteria the criteria to evaluate.
     * @param record the value to evaluate against the criteria.
     * @return whether the record should be selected for this specific criteria.
     */
    private boolean evaluateCriteria(FilterConfiguration.Criteria criteria, IndexedRecord record) {
        // This is the logical operation applied to multiple values applied inside ONE specific filter criteria.
        // When using a complex av expression, one accessor can read multiple values.
        // (i.e. ALL means that all values must evaluate to true.)
        LogicalOpType fieldOp = LogicalOpType.ALL;

        // Starting point for aggregating the logical operations.
        boolean aggregate = fieldOp.createAggregate();

        if (StringUtils.isEmpty(criteria.getColumnName())) {
            return false;
        }

        List<Object> values = getInputFields(record, criteria.getColumnName());

        if (ConditionsRowConstant.Function.COUNT.equals(criteria.getFunction())) {
            values = Arrays.asList((Object) values.size());
        } else if (values.size() == 0) {
            // If the function is not COUNT and no values are returned, then consider the criteria not matched.
            return false;
        }

        // Apply all of the criteria.
        for (Object value : values) {
            aggregate = fieldOp.combineAggregate(aggregate, checkCondition(value, criteria));
            if (fieldOp.canShortCircuit(aggregate))
                break;
        }

        return aggregate;
    }

    private <T extends Comparable<T>> Boolean checkCondition(Object inputValue, FilterConfiguration.Criteria filter) {
        ConditionsRowConstant.Function function = filter.getFunction();
        ConditionsRowConstant.Operator conditionOperator = filter.getOperator();
        String referenceValue = filter.getValue();

        // Apply the transformation function on the input value
        inputValue = FilterUtils.applyFunction(inputValue, function);

        if (referenceValue != null) {
            // TODO: do not cast the reference value at each comparison
            Class<T> inputValueClass = TypeConverterUtils.getComparableClass(inputValue);
            if (inputValueClass != null) {
                T convertedReferenceValue = TypeConverterUtils.parseTo(referenceValue, inputValueClass);
                return FilterUtils.compare(inputValueClass.cast(inputValue), conditionOperator, convertedReferenceValue);
            } else {
                return FilterUtils.compare(inputValue.toString(), conditionOperator, referenceValue);
            }
        } else {
            if (ConditionsRowConstant.Operator.EQUAL.equals(conditionOperator)) {
                return inputValue == null;
            } else { // Not Equals
                return inputValue != null;
            }
        }
    }

    private List<Object> getInputFields(IndexedRecord inputRecord, String columnName) {
        // Adapt non-avpath syntax to avpath.
        // TODO: This should probably not be automatic, use the actual syntax.
        if (!columnName.startsWith("."))
            columnName = "." + columnName;
        Try<scala.collection.immutable.List<Evaluator.Ctx>> result = wandou.avpath.package$.MODULE$.select(inputRecord,
                columnName);
        List<Object> values = new ArrayList<>();
        if (result.isSuccess()) {
            for (Evaluator.Ctx ctx : JavaConversions.asJavaCollection(result.get())) {
                values.add(ctx.value());
            }
        } else {
            // Evaluating the expression failed, and we can handle the exception.
            Throwable t = result.failed().get();
            throw ProcessingErrorCode.createAvpathSyntaxError(t, columnName, -1);
        }
        return values;
    }

    /**
     * A function that returns the exact same result as NOT FilterRowPredicate.
     */
    public static class Negate extends FilterPredicate {

        Negate(FilterConfiguration configuration) {
            super(configuration);
        }

        @Override
        public Boolean apply(IndexedRecord input) {
            return !super.apply(input);
        }
    }
}
