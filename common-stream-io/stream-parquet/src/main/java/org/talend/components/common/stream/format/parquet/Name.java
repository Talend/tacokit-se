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
package org.talend.components.common.stream.format.parquet;

import java.util.Optional;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@EqualsAndHashCode
@RequiredArgsConstructor
public class Name {

    private static char separator = '!';

    @Getter
    private final String name;

    @Getter
    private final String rawName;

    public static Name fromParquetName(final String parquetName) {
        final int posSep = parquetName.indexOf(Name.separator);
        if (posSep < 0) {
            return new Name(parquetName, null);
        }
        String rawName = parquetName.substring(0, posSep);
        if (rawName.length() == 0) {
            rawName = null;
        }
        String name = parquetName.substring(posSep + 1);
        if (name.length() == 0) {
            name = null;
        }
        return new Name(name, rawName);
    }

    public String parquetName() {
        return Optional.ofNullable(this.rawName).orElse("") + Name.separator + Optional.ofNullable(this.name).orElse("");
    }
}
