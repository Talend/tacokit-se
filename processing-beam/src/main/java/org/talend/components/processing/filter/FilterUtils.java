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

class FilterUtils {

    static <T extends Comparable<T>> Boolean compare(T inputValue, ConditionsRowConstant.Operator operator, T referenceValue) {
        if (ConditionsRowConstant.Operator.EQUAL.equals(operator)) {
            return inputValue.compareTo(referenceValue) == 0;
        } else if (ConditionsRowConstant.Operator.NOT_EQUAL.equals(operator)) {
            return inputValue.compareTo(referenceValue) != 0;
        } else if (ConditionsRowConstant.Operator.LOWER.equals(operator)) {
            return inputValue.compareTo(referenceValue) < 0;
        } else if (ConditionsRowConstant.Operator.LOWER_OR_EQUAL.equals(operator)) {
            return inputValue.compareTo(referenceValue) <= 0;
        } else if (ConditionsRowConstant.Operator.GREATER.equals(operator)) {
            return inputValue.compareTo(referenceValue) > 0;
        } else if (ConditionsRowConstant.Operator.GREATER_OR_EQUAL.equals(operator)) {
            return inputValue.compareTo(referenceValue) >= 0;
        } else if (ConditionsRowConstant.Operator.MATCH.equals(operator)) {
            return TypeConverterUtils.parseToString(inputValue).matches(TypeConverterUtils.parseToString(referenceValue));
        } else if (ConditionsRowConstant.Operator.NOT_MATCH.equals(operator)) {
            return !TypeConverterUtils.parseToString(inputValue).matches(TypeConverterUtils.parseToString(referenceValue));
        } else if (ConditionsRowConstant.Operator.CONTAINS.equals(operator)) {
            return TypeConverterUtils.parseToString(inputValue).contains(TypeConverterUtils.parseToString(referenceValue));
        } else if (ConditionsRowConstant.Operator.NOT_CONTAINS.equals(operator)) {
            return !TypeConverterUtils.parseToString(inputValue).contains(TypeConverterUtils.parseToString(referenceValue));
        }
        return false;
    }

    static Object applyFunction(Object inputValue, ConditionsRowConstant.Function function) {
        if (ConditionsRowConstant.Function.EMPTY.equals(function)) {
            return inputValue;
        } else if (ConditionsRowConstant.Function.ABS_VALUE.equals(function)) {
            if (inputValue instanceof Integer) {
                return Math.abs((Integer) inputValue);
            } else if (inputValue instanceof Long) {
                return Math.abs((Long) inputValue);
            } else if (inputValue instanceof Float) {
                return Math.abs((Float) inputValue);
            } else if (inputValue instanceof Double) {
                return Math.abs((Double) inputValue);
            } else {
                // TODO error case
                return inputValue;
            }
        } else if (ConditionsRowConstant.Function.LOWER_CASE.equals(function)) {
            return inputValue.toString().toLowerCase();
        } else if (ConditionsRowConstant.Function.UPPER_CASE.equals(function)) {
            return inputValue.toString().toUpperCase();
        } else if (ConditionsRowConstant.Function.FIRST_CHARACTER_LOWER_CASE.equals(function)) {
            return inputValue.toString().substring(0, 1).toLowerCase();
        } else if (ConditionsRowConstant.Function.FIRST_CHARACTER_UPPER_CASE.equals(function)) {
            return inputValue.toString().substring(0, 1).toUpperCase();
        } else if (ConditionsRowConstant.Function.LENGTH.equals(function)) {
            return inputValue.toString().length();
        } else {
            // TODO error case
            return inputValue;
        }
    }

}
