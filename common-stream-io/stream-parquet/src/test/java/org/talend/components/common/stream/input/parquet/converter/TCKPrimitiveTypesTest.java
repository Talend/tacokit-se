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

import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Schema;

class TCKPrimitiveTypesTest {

    @Test
    void toTCKType() {
        Assertions.assertEquals(Schema.Type.INT, this.extract(PrimitiveType.PrimitiveTypeName.INT32));
        Assertions.assertEquals(Schema.Type.LONG, this.extract(PrimitiveType.PrimitiveTypeName.INT64));
        Assertions.assertEquals(Schema.Type.BOOLEAN, this.extract(PrimitiveType.PrimitiveTypeName.BOOLEAN));
        Assertions.assertEquals(Schema.Type.FLOAT, this.extract(PrimitiveType.PrimitiveTypeName.FLOAT));
        Assertions.assertEquals(Schema.Type.DOUBLE, this.extract(PrimitiveType.PrimitiveTypeName.DOUBLE));
        Assertions.assertEquals(Schema.Type.BYTES, this.extract(PrimitiveType.PrimitiveTypeName.BINARY));
    }

    private Schema.Type extract(final PrimitiveType.PrimitiveTypeName name) {
        final PrimitiveType pt = new PrimitiveType(Type.Repetition.OPTIONAL, name, "");
        return TCKPrimitiveTypes.toTCKType(pt);
    }
}