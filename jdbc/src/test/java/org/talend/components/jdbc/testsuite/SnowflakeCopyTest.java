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
package org.talend.components.jdbc.testsuite;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.service.SnowflakeCopyService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.ServiceInjectionRule;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.jdbc")
public class SnowflakeCopyTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.jdbc");

    @Rule
    public final ServiceInjectionRule injections = new ServiceInjectionRule(COMPONENT_FACTORY, this);

    private RecordBuilderFactory recordBuilderFactory = COMPONENT_FACTORY.findService(RecordBuilderFactory.class);

    @Test
    public void createTmpDirTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        Method createWorkDir = SnowflakeCopyService.class.getDeclaredMethod("createWorkDir");
        createWorkDir.setAccessible(true);
        Path path = (Path) createWorkDir.invoke(snowflakeCopyService);
        Assertions.assertTrue(path.toFile().exists());
        Files.delete(path);
    }

    @Test
    public void cleanTmpFile() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        Method createWorkDir = SnowflakeCopyService.class.getDeclaredMethod("createWorkDir");
        createWorkDir.setAccessible(true);
        Path path2 = (Path) createWorkDir.invoke(snowflakeCopyService);
        snowflakeCopyService.cleanTmpFiles();
        Assertions.assertFalse(path2.toFile().exists());
    }

    @Test
    public void testTempTable() {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        String tableName = snowflakeCopyService.tmpTableName(
                "aVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryVeryLongName");
        Assertions.assertTrue(tableName.length() < 256);
    }

    @Test
    public void testSplitRecords() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        try {
            Method createWorkDir = SnowflakeCopyService.class.getDeclaredMethod("createWorkDir");
            createWorkDir.setAccessible(true);
            Path path = (Path) createWorkDir.invoke(snowflakeCopyService);
            Method splitRecords = SnowflakeCopyService.class.getDeclaredMethod("splitRecords", Path.class, List.class);
            splitRecords.setAccessible(true);
            List<?> chunks = (List<?>) splitRecords.invoke(snowflakeCopyService, path, createData(300000));
            Assertions.assertEquals(2, chunks.size());
        } finally {
            snowflakeCopyService.cleanTmpFiles();
        }
    }

    @Test
    public void testGetColumnNamesList() throws Exception {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        try {
            Method createWorkDir = SnowflakeCopyService.class.getDeclaredMethod("createWorkDir");
            createWorkDir.setAccessible(true);
            Path path = (Path) createWorkDir.invoke(snowflakeCopyService);
            Method splitRecords = SnowflakeCopyService.class.getDeclaredMethod("splitRecords", Path.class, List.class);
            splitRecords.setAccessible(true);
            List<?> chunks = (List<?>) splitRecords.invoke(snowflakeCopyService, path, createData(100));

            Method columnNames = SnowflakeCopyService.class.getDeclaredMethod("getColumnNamesList", List.class);
            columnNames.setAccessible(true);

            String columnNamesList = (String) columnNames.invoke(snowflakeCopyService, chunks);
            String expectedString = "(\"id\",\"firstname\",\"lastname\",\"address\",\"enrolled\",\"zip\",\"state\")";
            Assertions.assertEquals(expectedString, columnNamesList);

            chunks = (List<?>) splitRecords.invoke(snowflakeCopyService, path,
                    Arrays.asList(recordBuilderFactory.newRecordBuilder().withInt("id", 1).build()));
            columnNamesList = (String) columnNames.invoke(snowflakeCopyService, chunks);
            expectedString = "(\"id\")";
            Assertions.assertEquals(expectedString, columnNamesList);

            columnNamesList = (String) columnNames.invoke(snowflakeCopyService, (List<?>) null);
            Assertions.assertEquals("", columnNamesList);
        } finally {
            snowflakeCopyService.cleanTmpFiles();
        }
    }

    @Test
    public void testJoinFileNamesString() throws Exception {
        SnowflakeCopyService snowflakeCopyService = new SnowflakeCopyService();
        try {
            Method createWorkDir = SnowflakeCopyService.class.getDeclaredMethod("createWorkDir");
            createWorkDir.setAccessible(true);
            Path path = (Path) createWorkDir.invoke(snowflakeCopyService);
            Method splitRecords = SnowflakeCopyService.class.getDeclaredMethod("splitRecords", Path.class, List.class);
            splitRecords.setAccessible(true);
            List<?> chunks = (List<?>) splitRecords.invoke(snowflakeCopyService, path, createData(300000));

            Method joinFileNames = SnowflakeCopyService.class.getDeclaredMethod("joinFileNamesString", List.class);
            joinFileNames.setAccessible(true);

            String joinFileNamesString = (String) joinFileNames.invoke(snowflakeCopyService, chunks);
            final Pattern pattern = Pattern.compile("\\(('[/\\\\A-Za-z_0-9.-]*.csv.gz')(,'[/\\\\A-Za-z_0-9.-]*.csv.gz')*\\)");
            Assertions.assertTrue(pattern.matcher(joinFileNamesString).matches());
        } finally {
            snowflakeCopyService.cleanTmpFiles();
        }
    }

    List<Record> createData(int i) {
        List<Record> records = new ArrayList<Record>(i);
        for (; i > 0; i--) {
            Record record = recordBuilderFactory.newRecordBuilder() //
                    .withInt("id", i) //
                    .withString("firstname", "firstfirst") //
                    .withString("lastname", "lastlast") //
                    .withString("address", "addressaddr") //
                    .withString("enrolled", "Datedsldsk") //
                    .withString("zip", "89100") //
                    .withString("state", "YO") //
                    .build();
            records.add(record);
        }
        return records;
    }

}
