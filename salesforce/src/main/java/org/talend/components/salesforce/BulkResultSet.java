/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class BulkResultSet {

    private final com.csvreader.CsvReader reader;

    private final List<String> header;

    public BulkResultSet(com.csvreader.CsvReader reader, List<String> header) {
        this.reader = reader;
        this.header = header;
    }

    public Map<String, String> next() {
        try {
            boolean hasNext = reader.readRecord();
            Map<String, String> result = null;
            String[] row;
            if (hasNext) {
                if ((row = reader.getValues()) != null) {
                    result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
                    for (int i = 0; i < this.header.size(); i++) {
                        // We replace the . with _ to add support of relationShip Queries
                        // The relationShip Queries Use . in Salesforce and we use _ in Talend (Studio)
                        // So Account.Name in SF will be Account_Name in Talend
                        result.put(header.get(i).replace('.', '_'), row[i]);

                    }
                    return result;
                } else {
                    return next();
                }
            } else {
                this.reader.close();
            }
            return null;
        } catch (IOException e) {
            this.reader.close();
            throw new IllegalStateException(e);
        }
    }

}