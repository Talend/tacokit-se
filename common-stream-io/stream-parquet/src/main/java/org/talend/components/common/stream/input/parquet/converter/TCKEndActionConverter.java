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

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class TCKEndActionConverter extends GroupConverter {

    @FunctionalInterface
    public interface Action {

        void execute();
    }

    private final Converter innerConverter;

    private final Action endAction;

    public TCKEndActionConverter(Converter innerConverter) {
        this(innerConverter, null);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return this.innerConverter;
    }

    @Override
    public void start() {
        log.info("start");
    }

    @Override
    public void end() {
        log.info("end");
        if (this.endAction != null) {
            this.endAction.execute();
        }
        if (this.innerConverter instanceof TCKArrayPrimitiveConverter) {
            ((TCKArrayPrimitiveConverter) this.innerConverter).end();
        }
    }
}
