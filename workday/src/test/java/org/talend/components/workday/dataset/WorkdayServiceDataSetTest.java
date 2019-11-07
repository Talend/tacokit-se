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
package org.talend.components.workday.dataset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.workday.dataset.WorkdayServiceDataSet.Parameter;

import java.util.ArrayList;
import java.util.Map;

class WorkdayServiceDataSetTest {

    @Test
    void getServiceToCall() {
        WorkdayServiceDataSet ds = new WorkdayServiceDataSet();
        ds.setService("/w1");

        Assertions.assertEquals("w1", ds.getServiceToCall());

        Parameter p1 = new Parameter();
        p1.setType(Parameter.Type.Query);
        p1.setName("p1");
        p1.setValue("v1");
        ds.setParameters(new WorkdayServiceDataSet.Parameters());
        ds.getParameters().setParametersList(new ArrayList<>());
        ds.getParameters().getParametersList().add(p1);

        Assertions.assertEquals("w1", ds.getServiceToCall());

        this.addPath(ds.getParameters(), "PATH_1", "path1");
        this.addPath(ds.getParameters(), "PATH_2", "path2");

        Assertions.assertEquals("w1", ds.getServiceToCall());

        ds.setService("{PATH_1}/serv{PATH_2}/ss/{PATH_3}");

        Assertions.assertEquals("path1/servpath2/ss/{PATH_3}", ds.getServiceToCall());

        this.addPath(ds.getParameters(), "PATH_3", "path3");
        Assertions.assertEquals("path1/servpath2/ss/path3", ds.getServiceToCall());
    }

    @Test
    void extractQueryParam() {
        WorkdayServiceDataSet ds = new WorkdayServiceDataSet();
        Assertions.assertTrue(ds.extractQueryParam().isEmpty());
        ds.setParameters(new WorkdayServiceDataSet.Parameters());

        this.addPath(ds.getParameters(), "PATH_1", "path1");
        Assertions.assertTrue(ds.extractQueryParam().isEmpty());

        this.addQuery(ds.getParameters(), "par1", "v1");
        this.addQuery(ds.getParameters(), "par2", "v2");

        final Map<String, String> queryParams = ds.extractQueryParam();
        Assertions.assertEquals(2, queryParams.size());
        Assertions.assertEquals("v1", queryParams.get("par1"));
        Assertions.assertEquals("v2", queryParams.get("par2"));

    }

    private void addQuery(WorkdayServiceDataSet.Parameters params, String name, String value) {
        this.addParam(params, Parameter.Type.Query, name, value);
    }

    private void addPath(WorkdayServiceDataSet.Parameters params, String name, String value) {
        this.addParam(params, Parameter.Type.Path, name, value);
    }

    private void addParam(WorkdayServiceDataSet.Parameters params, Parameter.Type ptype, String name, String value) {
        final Parameter param = new Parameter();
        param.setType(ptype);
        param.setName(name);
        param.setValue(value);

        if (params.getParametersList() == null) {
            params.setParametersList(new ArrayList<>());
        }

        params.getParametersList().add(param);
    }

}