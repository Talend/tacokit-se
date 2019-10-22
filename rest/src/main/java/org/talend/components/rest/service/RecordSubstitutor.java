/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.rest.service;

import org.talend.components.common.text.Substitutor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.RecordPointerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public class RecordSubstitutor extends Substitutor {

    public RecordSubstitutor(final String prefix, final String suffix, final Record record,
            final RecordPointerFactory recordPointerFactory) {
        this(prefix, suffix, record, recordPointerFactory, new HashMap<>());
    }

    public RecordSubstitutor(final String prefix, final String suffix, final Record record,
            final RecordPointerFactory recordPointerFactory, Map<String, Optional<String>> intialCache) {

        super(prefix, suffix, intialCache, new Function<String, String>() {

            @Override
            public String apply(final String key) {
                String value = null;

                try {
                    value = ofNullable(recordPointerFactory.apply(key).getValue(record, Object.class)).filter(v -> {
                        if (Record.class.isInstance(v) || Collection.class.isInstance(v)) {
                            throw new IllegalArgumentException("Invalid record pointer: " + v);
                        }
                        return true;
                    }).map(String::valueOf).orElse(null); // If other than null, then ':-' default syntax in place holder
                    // is not taken into account
                } catch (IllegalArgumentException e) {
                    // If pointer can't retrieve value we prefer return null tso that default value after ':-' can be use.
                    value = null;
                }
                return value;
            }
        });
    }

}
