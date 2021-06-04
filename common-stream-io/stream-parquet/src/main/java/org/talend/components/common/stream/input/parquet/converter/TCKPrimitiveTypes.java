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
package org.talend.components.common.stream.input.parquet.converter;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.talend.sdk.component.api.record.Schema;

public class TCKPrimitiveTypes {

    public static Schema.Type toTCKType(final PrimitiveType pt) {
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.INT32) {
            return Schema.Type.INT;
        }
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.INT64) {
            return Schema.Type.LONG;
        }
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.BOOLEAN) {
            return Schema.Type.BOOLEAN;
        }
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.FLOAT) {
            return Schema.Type.FLOAT;
        }
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.DOUBLE) {
            return Schema.Type.DOUBLE;
        }
        if (pt.getPrimitiveTypeName() == PrimitiveTypeName.BINARY) {
            if (pt.getLogicalTypeAnnotation() == LogicalTypeAnnotation.stringType()) {
                return Schema.Type.STRING;
            }
            return Schema.Type.BYTES;
        }
        return null;
    }

}
