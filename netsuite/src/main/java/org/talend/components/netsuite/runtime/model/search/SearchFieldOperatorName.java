/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.netsuite.runtime.model.search;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class SearchFieldOperatorName {

    private String dataType;

    private String name;

    public SearchFieldOperatorName(String qualifiedName) {
        int i = qualifiedName.indexOf('.');
        if (i == -1) {
            this.dataType = qualifiedName;
            this.name = null;
        } else {
            String tempDataType = qualifiedName.substring(0, i);
            if (tempDataType.isEmpty()) {
                throw new IllegalArgumentException("Invalid operator data type: " + "'" + tempDataType + "'");
            }
            String tempName = qualifiedName.substring(i + 1);
            if (tempName.isEmpty()) {
                throw new IllegalArgumentException("Invalid operator name: " + "'" + tempName + "'");
            }

            this.dataType = tempDataType;
            this.name = tempName;
        }
    }

    public String getQualifiedName() {
        return name != null ? dataType + '.' + name : dataType;
    }
}
