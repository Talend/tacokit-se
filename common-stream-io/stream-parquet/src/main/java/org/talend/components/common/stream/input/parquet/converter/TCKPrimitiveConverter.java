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

import java.util.function.Consumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class TCKPrimitiveConverter extends PrimitiveConverter {

    private final Consumer<Object> setter;

    @Override
    public void addBoolean(boolean value) {
        this.setValue(value);
    }

    @Override
    public void addInt(int value) {
        this.setValue(value);
    }

    @Override
    public void addBinary(Binary value) {
        this.setValue(value.toStringUsingUTF8());
    }

    @Override
    public void addDouble(double value) {
        this.setValue(value);
    }

    @Override
    public void addFloat(float value) {
        this.setValue(value);
    }

    @Override
    public void addLong(long value) {
        this.setValue(value);
    }

    private void setValue(final Object value) {
        if (value != null) {
            this.setter.accept(value);
        }
    }
}
