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
package org.talend.components.workday.dataset;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RAASLayoutTest {

    @Test
    void getServiceToCall() {
        RAASLayout layout = new RAASLayout();
        layout.setUser("myuser");
        layout.setReport("myreport");
        Assertions.assertEquals("raas/myuser/myreport", layout.getServiceToCall());
    }

    @Test
    void serialize() throws IOException, ClassNotFoundException {

        RAASLayout layout = new RAASLayout();
        layout.setUser("myuser");
        layout.setReport("myreport");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(baos);
        out.writeObject(layout);

        ByteArrayInputStream b = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream input = new ObjectInputStream(b);
        Object obj = input.readObject();
        Assertions.assertNotNull(obj);
        Assertions.assertTrue(obj instanceof RAASLayout);
        Assertions.assertEquals(layout, obj);
    }
}