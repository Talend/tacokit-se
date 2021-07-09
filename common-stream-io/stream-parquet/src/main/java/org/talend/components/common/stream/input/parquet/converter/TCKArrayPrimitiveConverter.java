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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import org.talend.sdk.component.api.record.Schema;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TCKArrayPrimitiveConverter extends TCKPrimitiveConverter {

    /**
     * This class because in TCK,
     */
    @RequiredArgsConstructor
    public static class CollectionSetter {

        private final Consumer<Collection<Object>> arraySetter;

        private final Schema.Entry field;

        private List<Object> objects = new ArrayList<>();

        public void add(Object object) {
            final Object realValue = TCKConverter.realValue(field.getElementSchema().getType(), object);
            this.objects.add(realValue);
        }

        public void end() {
            log.info("Call setter size : " + this.objects.size());
            this.arraySetter.accept(this.objects);
            this.objects = new ArrayList<>();
        }
    }

    private final CollectionSetter collectionSetter;

    public TCKArrayPrimitiveConverter(final CollectionSetter collectionSetter) {
        super(collectionSetter::add);
        this.collectionSetter = collectionSetter;
    }

    public void end() {
        log.info("end");
        this.collectionSetter.end();
    }
}
