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

class ConditionsRowConstant {

    public enum Function {
        EMPTY("EMPTY"),
        ABS_VALUE("ABS_VALUE"),
        LOWER_CASE("LC"),
        UPPER_CASE("UC"),
        FIRST_CHARACTER_LOWER_CASE("LCFIRST"),
        FIRST_CHARACTER_UPPER_CASE("UCFIRST"),
        LENGTH("LENGTH"),
        COUNT("COUNT");

        private final String value;

        Function(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }

    public enum Operator {
        EQUAL("=="),
        NOT_EQUAL("!="),
        LOWER("<"),
        LOWER_OR_EQUAL("<="),
        GREATER(">"),
        GREATER_OR_EQUAL(">="),
        MATCH("MATCH"),
        NOT_MATCH("NOT_MATCH"),
        CONTAINS("CONTAINS"),
        NOT_CONTAINS("NOT_CONTAINS");

        private final String value;

        Operator(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
