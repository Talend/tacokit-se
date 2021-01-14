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
package org.talend.components.couchbase;

import com.couchbase.client.java.document.json.JsonObject;
import lombok.Data;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.SchemaImpl;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Data
public class TestData {

    private String colId = "id";

    private int colIntMin = Integer.MIN_VALUE;

    private int colIntMax = Integer.MAX_VALUE;

    private long colLongMin = Long.MIN_VALUE;

    private long colLongMax = Long.MAX_VALUE;

    private float colFloatMin = Float.MIN_VALUE;

    private float colFloatMax = Float.MAX_VALUE;

    private double colDoubleMin = Double.MIN_VALUE;

    private double colDoubleMax = Double.MAX_VALUE;

    private boolean colBoolean = Boolean.TRUE;

    private ZonedDateTime colDateTime = ZonedDateTime.of(2018, 10, 30, 10, 30, 59, 0, ZoneId.of("UTC"));

    private List<String> colList = new ArrayList<>(Arrays.asList("data1", "data2", "data3"));

    public JsonObject createJson(String id) {
        JsonObject js = JsonObject.create();
        js.put("t_string", id);
        js.put("t_int_min", getColIntMin());
        js.put("t_int_max", getColIntMax());
        js.put("t_long_min", getColLongMin());
        js.put("t_long_max", getColLongMax());
        js.put("t_float_min", getColFloatMin());
        js.put("t_float_max", getColFloatMax());
        js.put("t_double_min", getColDoubleMin());
        js.put("t_double_max", getColDoubleMax());
        js.put("t_boolean", isColBoolean());
        js.put("t_datetime", getColDateTime().toString());
        js.put("t_array", getColList());
        return js;
    }

    public String createParameterizedJsonString() {
        return "{\"t_string\":$t_string,\"t_int_min\":$t_int_min,\"t_int_max\":$t_int_max,"
                + "\"t_long_min\":$t_long_min,\"t_long_max\":$t_long_max,"
                + "\"t_float_min\":$t_float_min,\"t_float_max\":$t_float_max,"
                + "\"t_double_min\":$t_double_min,\"t_double_max\":$t_double_max,"
                + "\"t_boolean\":$t_boolean,\"t_datetime\":$t_datetime,\"t_array\":$t_array}";
    }

    public Record createRecord(RecordBuilderFactory recordBuilderFactory, String id) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        SchemaImpl arrayInnerSchema = new SchemaImpl();
        arrayInnerSchema.setType(Schema.Type.STRING);
        return recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), id)
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), getColIntMin())
                .withInt(entryBuilder.withName("t_int_max").withType(Schema.Type.INT).build(), getColIntMax())
                .withLong(entryBuilder.withName("t_long_min").withType(Schema.Type.LONG).build(), getColLongMin())
                .withLong(entryBuilder.withName("t_long_max").withType(Schema.Type.LONG).build(), getColLongMax())
                .withFloat(entryBuilder.withName("t_float_min").withType(Schema.Type.FLOAT).build(), getColFloatMin())
                .withFloat(entryBuilder.withName("t_float_max").withType(Schema.Type.FLOAT).build(), getColFloatMax())
                .withDouble(entryBuilder.withName("t_double_min").withType(Schema.Type.DOUBLE).build(), getColDoubleMin())
                .withDouble(entryBuilder.withName("t_double_max").withType(Schema.Type.DOUBLE).build(), getColDoubleMax())
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), isColBoolean())
                .withDateTime(entryBuilder.withName("t_datetime").withType(Schema.Type.DATETIME).build(), getColDateTime())
                .withArray(
                        entryBuilder.withName("t_array").withType(Schema.Type.ARRAY).withElementSchema(arrayInnerSchema).build(),
                        getColList())
                .build();
    }
}
