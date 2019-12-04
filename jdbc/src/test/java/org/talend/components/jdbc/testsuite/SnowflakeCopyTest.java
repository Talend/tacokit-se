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
package org.talend.components.jdbc.testsuite;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

public class SnowflakeCopyTest {

    @Test
    public void createTmpDirTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        Method createWorkDir = SnowflakeCopy.class.getDeclaredMethod("createWorkDir");
        createWorkDir.setAccessible(true);
        Path path = (Path) createWorkDir.invoke(null);
        Assertions.assertTrue(path.toFile().exists());
        Files.delete(path);
    }

    @Test
    public void cleanTmpFile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method createWorkDir = SnowflakeCopy.class.getDeclaredMethod("createWorkDir");
        createWorkDir.setAccessible(true);
        Path path2 = (Path) createWorkDir.invoke(null);
        SnowflakeCopy.cleanTmpFiles();
        Assertions.assertFalse(path2.toFile().exists());
    }

}
