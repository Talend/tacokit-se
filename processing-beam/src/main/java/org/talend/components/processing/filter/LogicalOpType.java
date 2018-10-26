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

/**
 * Describe how to combine criteria, corresponds to logical boolean operations.
 *
 * This class provides methods to abstract the logical operation, using the following pattern.
 *
 * <code>
 * // Combine the following values with the logical operator.
 * boolean[] valuesToAggregate = {true, false, true};
 * LogicalOp op = LogicalOpp.ALL;
 *
 * // Start with an aggregate state that describes the operation before any value
 * // is applied.
 * boolean aggregate = op.createAggregate();
 * for (boolean nextValue: valuesToAggregate) {
 *     // Add each consecutive value to the aggregate state.
 *     aggregate = op.combineAggregate(aggregate, nextValue);
 *     // Optional: if the operation can be short-circuited based on the current state.
 *     if (op.canShortCircuit(aggregate))
 *         break;
 * }
 *
 * // The aggregate state is the value of evaluating the values with the operator.
 * </code>
 */
public enum LogicalOpType {
    /**
     * All criteria must be true for the record to be selected (AND).
     */
    ALL,
    /**
     * At least one criteria must be true for the record to be selected (OR).
     */
    ANY,
    /**
     * No criteria can be true for the record to be selected (NOR).
     */
    NONE;

    /**
     * @return an initial aggregate state used to combine boolean values with this operation.
     */
    public boolean createAggregate() {
        switch (this) {
        case ALL:
            return true;
        case ANY:
            return false;
        case NONE:
            return true;
        default:
            return false;
        }
    }

    /**
     * Add the value to the aggregate state.
     *
     * @return the new aggregate state incorporating the value.
     */
    public boolean combineAggregate(boolean aggregate, boolean value) {
        switch (this) {
        case ALL:
            return aggregate && value;
        case ANY:
            return aggregate || value;
        case NONE:
            return aggregate && !value;
        default:
            return false;
        }
    }

    /**
     * Determine whether further values can change the aggregate state.
     *
     * @return true when no further combineAggregate calls can possibly change the given aggregate state.
     */
    public boolean canShortCircuit(boolean aggregate) {
        switch (this) {
        case ALL:
            return !aggregate;
        case ANY:
            return aggregate;
        case NONE:
            return !aggregate;
        default:
            return true;
        }
    }
}
