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
package org.talend.components.common.stream.output.parquet.converter;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.talend.sdk.component.api.record.Schema;

public class ParquetPrimitiveTypes {

    public static PrimitiveTypeName toParquetType(final Schema.Type tckType) {
        if (tckType == Schema.Type.INT) {
            return PrimitiveTypeName.INT32;
        }
        if (tckType == Schema.Type.LONG) {
            return PrimitiveTypeName.INT64;
        }
        if (tckType == Schema.Type.BOOLEAN) {
            return PrimitiveTypeName.BOOLEAN;
        }
        if (tckType == Schema.Type.FLOAT) {
            return PrimitiveTypeName.FLOAT;
        }
        if (tckType == Schema.Type.DOUBLE) {
            return PrimitiveTypeName.DOUBLE;
        }
        if (tckType == Schema.Type.BYTES) {
            return PrimitiveTypeName.BINARY;
        }
        if (tckType == Schema.Type.STRING) {
            return PrimitiveTypeName.BINARY;
        }
        if (tckType == Schema.Type.DATETIME) {
            return PrimitiveTypeName.INT64;
        }
        return null;
    }

}
