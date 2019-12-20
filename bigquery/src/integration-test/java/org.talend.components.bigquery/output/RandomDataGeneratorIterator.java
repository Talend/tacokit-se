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
package org.talend.components.bigquery.output;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

public class RandomDataGeneratorIterator implements Iterator<Record>, Serializable {

    private int nbRecords;

    private AtomicInteger count = new AtomicInteger();

    private RecordBuilderFactory rbf;

    public RandomDataGeneratorIterator(int nbRecords, RecordBuilderFactory rbf) {
        this.nbRecords = nbRecords;
        this.rbf = rbf;
    }

    public RandomDataGeneratorIterator() {

    }

    @Override
    public boolean hasNext() {
        return count.intValue() < nbRecords;
    }

    @Override
    public Record next() {
        if (!hasNext()) {
            return null;
        }

        int countVal = count.incrementAndGet();

        // System.out.println(countVal + " / " + nbRecords);

        return rbf.newRecordBuilder().withInt("ID", countVal)
                .withString("NAME", Thread.currentThread().getName() + "_" + countVal).withFloat("VALUE_F", (float) Math.random())
                .withBoolean("IS_TRUE", countVal % 2 == 0).withTimestamp("TMSTP", System.currentTimeMillis())
                .withDateTime("A_DATE", new Date()).withDouble("A_NUMERIC", 0.).build();
    }

}
