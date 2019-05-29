package org.talend.components.onedrive;

import com.microsoft.graph.models.extensions.DriveItem;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.onedrive.helpers.StringHelper;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.components.onedrive.sources.create.OneDriveCreateConfiguration;
import org.talend.components.onedrive.sources.delete.OneDriveDeleteConfiguration;
import org.talend.components.onedrive.sources.get.OneDriveGetConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveListConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.components.onedrive.sources.put.OneDrivePutConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@DisplayName("Suite of test for the OneDrive components")
@WithComponents("org.talend.components.onedrive")
@ExtendWith(OneDriveTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OneDriveTestIT {

    private static final String TEMP_DIR = System.getProperty("java.io.tmpdir");

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    @Service
    private OneDriveHttpClientService oneDriveHttpClientService;

    private OneDriveTestExtension.TestContext testContext;

    @BeforeAll
    private void init(OneDriveTestExtension.TestContext testContext) {
        log.info("init service test");
        this.testContext = testContext;
    }

    @Test
    @DisplayName("List. Get files in directory")
    void listComponentGetFilesInDirectory() {
        log.info("Integration test 'List. Get files in directory' start ");
        OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setObjectPath("/integr-tests/get");
        // dataSet.setObjectType(OneDriveObjectType.DIRECTORY);
        dataSet.setRecursively(true);

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("onedrive-list", "OneDrive://List?" + config).component("collector", "test://collector")
                .connections().from("onedrive-list").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(2, res.size());
        Assertions.assertTrue("gettest1.txt,gettest2.txt".contains(res.get(0).getString("name")));
        Assertions.assertTrue("gettest1.txt,gettest2.txt".contains(res.get(1).getString("name")));
    }

    @Test
    @DisplayName("List. Get root")
    void listComponentGetRoot() {
        log.info("Integration test 'List. Get root' start ");
        OneDriveListConfiguration dataSet = new OneDriveListConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());

        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();
        Job.components().component("onedrive-list", "OneDrive://List?" + config).component("collector", "test://collector")
                .connections().from("onedrive-list").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        Assertions.assertEquals("root", res.iterator().next().getString("name"));
    }

    @Test
    @DisplayName("Create. Create folder")
    void createComponentCreateFolder() {
        log.info("Integration test 'Create. Create folder' start.");
        String fileName = "newFile.txt";
        String filePath = "integr-tests/create/dir1/dir1_2";
        // create config
        OneDriveCreateConfiguration dataSetCreate = new OneDriveCreateConfiguration();
        dataSetCreate.setDataSet(testContext.getDataSetLoginPassword());
        dataSetCreate.setCreateDirectoriesByList(false);
        dataSetCreate.setObjectType(OneDriveObjectType.FILE);
        dataSetCreate.setObjectPath(filePath + "/" + fileName);
        final String configCreate = configurationByExample().forInstance(dataSetCreate).configured().toQueryString();

        DriveItem root = oneDriveHttpClientService.getRoot(testContext.getDataSetLoginPassword().getDataStore());
        String parentId = root.id;
        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("parentId", parentId).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject));

        Job.components().component("emitter", "test://emitter").component("onedrive-create", "OneDrive://Create?" + configCreate)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-create")
                .from("onedrive-create").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(1, res.size());
        JsonObject newItem = res.get(0);
        Assertions.assertTrue(newItem.containsKey("file"));
        Assertions.assertEquals(fileName, newItem.getString("name"));
        Assertions.assertEquals("/drive/root:/" + filePath, newItem.getJsonObject("parentReference").getString("path"));
    }

    @Test
    @DisplayName("Create. Create folder list")
    void createComponentCreateFolderList() {
        log.info("Integration test 'Create. Create folder list' start.");
        // create config
        OneDriveCreateConfiguration dataSetCreate = new OneDriveCreateConfiguration();
        dataSetCreate.setDataSet(testContext.getDataSetLoginPassword());
        dataSetCreate.setCreateDirectoriesByList(true);
        final String configCreate = configurationByExample().forInstance(dataSetCreate).configured().toQueryString();

        JsonObject jsonObject1 = jsonBuilderFactory.createObjectBuilder().add("objectPath", "integr-tests/create/dir2").build();
        JsonObject jsonObject2 = jsonBuilderFactory.createObjectBuilder().add("objectPath", "integr-tests/create/dir3/dir3_1")
                .build();
        componentsHandler.setInputData(Arrays.asList(jsonObject1, jsonObject2));

        Job.components().component("emitter", "test://emitter").component("onedrive-create", "OneDrive://Create?" + configCreate)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-create")
                .from("onedrive-create").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(2, res.size());

        Map<String, String> resMap1 = new HashMap<>();
        resMap1.put("file", null);
        resMap1.put("name", "dir2");
        resMap1.put("parentPath", "/drive/root:/integr-tests/create");
        Map<String, String> resMap2 = new HashMap<>();
        resMap2.put("file", null);
        resMap2.put("name", "dir3_1");
        resMap2.put("parentPath", "/drive/root:/integr-tests/create/dir3");
        List<Map<String, String>> goodResult = Arrays.asList(resMap1, resMap2);

        List<Map<String, String>> result = res.stream().map(item -> {
            Map<String, String> resMap = new HashMap<>();
            resMap.put("file", item.containsKey("file") ? "" : null);
            resMap.put("name", item.getString("name"));
            resMap.put("parentPath", item.getJsonObject("parentReference").getString("path"));
            return resMap;
        }).collect(Collectors.toList());
        Assertions.assertEquals(goodResult, result);
    }

    @Test
    @DisplayName("Delete. Delete all files in folder")
    void deleteComponentAllFilesInFolder() throws IOException {
        log.info("Integration test 'Delete. All files in folder.");
        // create config
        OneDriveDeleteConfiguration dataSet = new OneDriveDeleteConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        DriveItem newFile = oneDriveHttpClientService.createItem(testContext.getDataSetLoginPassword().getDataStore(), null,
                OneDriveObjectType.FILE, "integr-tests/delete/dir1/newFile.txt");
        DriveItem newFolder = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                "integr-tests/delete/dir1");
        JsonObject jsonObject1 = jsonBuilderFactory.createObjectBuilder().add("id", newFile.id).build();
        JsonObject jsonObject2 = jsonBuilderFactory.createObjectBuilder().add("id", newFolder.id).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject1, jsonObject2));

        Job.components().component("emitter", "test://emitter").component("onedrive-delete", "OneDrive://Delete?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-delete")
                .from("onedrive-delete").to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);
        Assertions.assertEquals(2, res.size());
    }

    @Test
    @DisplayName("Get. Get files to folder")
    void getComponentFilesToFolder() throws IOException {
        log.info("Integration test 'Get. Files to folder. Destination: " + TEMP_DIR);
        // create config
        OneDriveGetConfiguration dataSet = new OneDriveGetConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setStoreFilesLocally(true);
        dataSet.setStoreDirectory(TEMP_DIR);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        String filePath1 = "integr-tests/get/gettest1.txt";
        String filePath2 = "integr-tests/get/gettest2.txt";
        DriveItem file1 = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                filePath1);
        DriveItem file2 = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                filePath2);
        JsonObject jsonObject1 = jsonBuilderFactory.createObjectBuilder().add("id", file1.id).build();
        JsonObject jsonObject2 = jsonBuilderFactory.createObjectBuilder().add("id", file2.id).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject1, jsonObject2));

        Job.components().component("emitter", "test://emitter").component("onedrive-get", "OneDrive://Get?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-get").from("onedrive-get")
                .to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        byte[] fileBytes1 = Files.readAllBytes(Paths.get(TEMP_DIR + "/" + filePath1));
        Assertions.assertEquals("get test file 1 content", new String(fileBytes1, StringHelper.STRING_CHARSET));
        byte[] fileBytes2 = Files.readAllBytes(Paths.get(TEMP_DIR + "/" + filePath2));
        Assertions.assertEquals("get test file 2 content", new String(fileBytes2, StringHelper.STRING_CHARSET));
    }

    @Test
    @DisplayName("Get. Get files to byte array")
    void getComponentFilesToByteArray() throws IOException {
        log.info("Integration test 'Get. Files to byte array.");
        // create config
        OneDriveGetConfiguration dataSet = new OneDriveGetConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setStoreFilesLocally(false);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        String filePath1 = "integr-tests/get/gettest1.txt";
        String fileContentOrigin1 = "get test file 1 content";
        String filePath2 = "integr-tests/get/gettest2.txt";
        String fileContentOrigin2 = "get test file 2 content";

        // create files
        oneDriveHttpClientService.putItemData(testContext.getDataSetLoginPassword().getDataStore(), filePath1,
                new ByteArrayInputStream(fileContentOrigin1.getBytes()), fileContentOrigin1.length());
        oneDriveHttpClientService.putItemData(testContext.getDataSetLoginPassword().getDataStore(), filePath2,
                new ByteArrayInputStream(fileContentOrigin2.getBytes()), fileContentOrigin2.length());

        DriveItem file1 = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                filePath1);
        DriveItem file2 = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                filePath2);
        JsonObject jsonObject1 = jsonBuilderFactory.createObjectBuilder().add("id", file1.id).build();
        JsonObject jsonObject2 = jsonBuilderFactory.createObjectBuilder().add("id", file2.id).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject1, jsonObject2));

        Job.components().component("emitter", "test://emitter").component("onedrive-get", "OneDrive://Get?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-get").from("onedrive-get")
                .to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        Assertions.assertEquals(2, res.size());
        Map<String, String> fileData = Collections
                .unmodifiableMap(res.stream().collect(Collectors.toMap(i -> i.getString("id"), i -> i.getString("payload"))));
        String fileContent1 = new String(Base64.getDecoder().decode(fileData.get(file1.id)), StringHelper.STRING_CHARSET);
        String fileContent2 = new String(Base64.getDecoder().decode(fileData.get(file2.id)), StringHelper.STRING_CHARSET);
        Assertions.assertEquals(fileContentOrigin1, fileContent1);
        Assertions.assertEquals(fileContentOrigin2, fileContent2);
    }

    @Test
    @DisplayName("Get. Get files to byte array")
    void getComponentFolder() throws IOException {
        log.info("Integration test 'Get. Files to byte array.");
        // create config
        OneDriveGetConfiguration dataSet = new OneDriveGetConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setStoreFilesLocally(false);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        String folderPath1 = "integr-tests/get";

        // create dir
        oneDriveHttpClientService.createItem(testContext.getDataSetLoginPassword().getDataStore(), null,
                OneDriveObjectType.DIRECTORY, folderPath1);

        DriveItem folder1 = oneDriveHttpClientService.getItemByPath(testContext.getDataSetLoginPassword().getDataStore(),
                folderPath1);
        JsonObject jsonObject3 = jsonBuilderFactory.createObjectBuilder().add("id", folder1.id).build();
        componentsHandler.setInputData(Arrays.asList(jsonObject3));

        Job.components().component("emitter", "test://emitter").component("onedrive-get", "OneDrive://Get?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-get").from("onedrive-get")
                .to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        Assertions.assertEquals(1, res.size());
    }

    @Test
    @DisplayName("Put. Put files from folder")
    void putComponentFilesFromFolder() throws IOException {
        log.info("Integration test 'Put. Files from folder. Source: " + TEMP_DIR);
        // create config
        OneDrivePutConfiguration dataSet = new OneDrivePutConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setDataSource(OneDrivePutConfiguration.DataSource.File);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        String folderPath = "integr-tests/put";
        String filePath1 = "integr-tests/put/puttest1.txt";
        String filePath2 = "integr-tests/put/puttest2.txt";
        if (!new File(TEMP_DIR, folderPath).exists()) {
            Files.createDirectories(Paths.get(TEMP_DIR + "/" + folderPath));
        }
        for (String filePath : new String[] { filePath1, filePath2 }) {
            try (InputStream stream = getClass().getResourceAsStream("/" + filePath);
                    OutputStream resStreamOut = new FileOutputStream(TEMP_DIR + "/" + filePath)) {
                int readBytes;
                byte[] buffer = new byte[4096];
                while ((readBytes = stream.read(buffer)) > 0) {
                    resStreamOut.write(buffer, 0, readBytes);
                }
            }
        }

        List<JsonObject> inputData = new ArrayList<>();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("itemPath", folderPath).addNull("localPath").build();
        inputData.add(jsonObject);
        for (String filePath : new String[] { filePath1, filePath2 }) {
            jsonObject = jsonBuilderFactory.createObjectBuilder().add("itemPath", filePath)
                    .add("localPath", TEMP_DIR + "/" + filePath).build();
            inputData.add(jsonObject);
        }
        componentsHandler.setInputData(inputData);

        Job.components().component("emitter", "test://emitter").component("onedrive-put", "OneDrive://Put?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-put").from("onedrive-put")
                .to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        Assertions.assertEquals(3, res.size());
    }

    @Test
    @DisplayName("Put. Put files from byte array")
    void putComponentFilesFromByteArray() throws IOException {
        log.info("Integration test 'Put. Files from byte array.");
        // create config
        OneDrivePutConfiguration dataSet = new OneDrivePutConfiguration();
        dataSet.setDataSet(testContext.getDataSetLoginPassword());
        dataSet.setDataSource(OneDrivePutConfiguration.DataSource.Content);
        final String config = configurationByExample().forInstance(dataSet).configured().toQueryString();

        String folderPath = "integr-tests/putbytes";
        String filePath1 = "integr-tests/putbytes/puttest1.txt";
        String filePath2 = "integr-tests/putbytes/puttest2.txt";
        Map<String, String> payloads = new HashMap<>();
        for (String filePath : new String[] { filePath1, filePath2 }) {
            try (InputStream stream = getClass().getResourceAsStream("/" + filePath);
                    ByteArrayOutputStream resStreamOut = new ByteArrayOutputStream()) {
                int readBytes;
                byte[] buffer = new byte[4096];
                while ((readBytes = stream.read(buffer)) > 0) {
                    resStreamOut.write(buffer, 0, readBytes);
                }
                payloads.put(filePath, Base64.getEncoder().encodeToString(resStreamOut.toByteArray()));
            }
        }

        List<JsonObject> inputData = new ArrayList<>();

        JsonObject jsonObject = jsonBuilderFactory.createObjectBuilder().add("itemPath", folderPath).addNull("payload").build();
        inputData.add(jsonObject);
        for (Map.Entry<String, String> payload : payloads.entrySet()) {
            jsonObject = jsonBuilderFactory.createObjectBuilder().add("itemPath", payload.getKey())
                    .add("payload", payload.getValue()).build();
            inputData.add(jsonObject);
        }
        componentsHandler.setInputData(inputData);

        Job.components().component("emitter", "test://emitter").component("onedrive-put", "OneDrive://Put?" + config)
                .component("collector", "test://collector").connections().from("emitter").to("onedrive-put").from("onedrive-put")
                .to("collector").build().run();
        final List<JsonObject> res = componentsHandler.getCollectedData(JsonObject.class);

        Assertions.assertEquals(3, res.size());
    }

}
