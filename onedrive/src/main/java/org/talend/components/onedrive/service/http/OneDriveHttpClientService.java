package org.talend.components.onedrive.service.http;

import com.microsoft.graph.concurrency.ChunkedUploadProvider;
import com.microsoft.graph.concurrency.IProgressCallback;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.DriveItemUploadableProperties;
import com.microsoft.graph.models.extensions.Folder;
import com.microsoft.graph.models.extensions.UploadSession;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.service.graphclient.GraphClient;
import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.sources.get.OneDriveGetConfiguration;
import org.talend.components.onedrive.sources.list.OneDriveObjectType;
import org.talend.sdk.component.api.service.Service;

import javax.json.JsonBuilderFactory;
import javax.json.JsonObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;

@Service
@Slf4j
public class OneDriveHttpClientService {

    public static final int RESPONSE_CODE_OK = 200;

    private static final int ERROR_CODE_ITEM_NOT_FOUND = 404;

    private static final int ERROR_CODE_CONFLICT = 409;

    private static final String DRIVE_ROOT_PATH = "/drive/root:";

    @Service
    private GraphClientService graphClientService;

    @Service
    private JsonBuilderFactory jsonBuilderFactory;

    public DriveItem getRoot(OneDriveDataStore dataStore) {
        log.debug("get root");
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        DriveItem root = graphClient.getRoot();
        return root;
    }

    public DriveItem getItem(OneDriveDataStore dataStore, String itemId) {
        log.debug("get item");
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        DriveItem item = graphClient.getDriveRequestBuilder().items(itemId).buildRequest().get();
        return item;
    }

    public IDriveItemCollectionPage getItemChildren(OneDriveDataStore dataStore, DriveItem parent) {
        log.debug("get item's children: " + parent.name);
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        IDriveItemCollectionPage pages = null;
        if (parent.folder != null && parent.folder.childCount > 0) {
            pages = graphClient.getDriveRequestBuilder().items(parent.id).children().buildRequest().get();
        }
        return pages;
    }

    public DriveItem getItemByPath(OneDriveDataStore dataStore, String path) {
        log.debug("get item by path: " + path);
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        DriveItem driveItem;
        if (path == null || path.isEmpty()) {
            driveItem = getRoot(dataStore);
        } else {
            driveItem = graphClient.getDriveRequestBuilder().root().itemWithPath(path).buildRequest().get();
        }
        return driveItem;
    }

    public DriveItem getItemByName(OneDriveDataStore dataStore, String parentId, String itemName) {
        log.debug("get item by name: " + itemName);
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        DriveItem driveItem;
        if (parentId == null || parentId.isEmpty()) {
            driveItem = graphClient.getDriveRequestBuilder().root().itemWithPath(itemName).buildRequest().get();
        } else {
            driveItem = graphClient.getDriveRequestBuilder().items(parentId).itemWithPath(itemName).buildRequest().get();
        }
        return driveItem;
    }

    /**
     * Create file or folder. Use '/' as a path delimiter
     *
     * @param parentId - parent item id
     * @param objectType - File or Directory
     * @param itemPath - full path to new item relatively to parent
     * @throws IOException
     */
    public DriveItem createItem(OneDriveDataStore dataStore, String parentId, OneDriveObjectType objectType, String itemPath)
            throws IOException {
        log.debug("create item: " + (parentId == null ? "root" : parentId));
        if (itemPath == null || itemPath.isEmpty()) {
            return null;
        }
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);

        if (parentId == null || parentId.isEmpty()) {
            parentId = getRoot(dataStore).id;
        }
        String[] pathParts = itemPath.split("/");
        String parentRelativePath = Arrays.stream(pathParts).limit(pathParts.length - 1).collect(Collectors.joining("/"));
        String itemName = pathParts[pathParts.length - 1];

        DriveItem newItem;
        // try to create item
        try {
            newItem = createItemInternal(graphClient, parentId, parentRelativePath, objectType, itemName);
            log.debug("new item " + newItem.name + " was created");
        } catch (GraphServiceException e) {
            newItem = handleCreateItemError(e, dataStore, parentId, objectType, itemPath, pathParts, itemName);
        }
        return newItem;
    }

    private DriveItem handleCreateItemError(GraphServiceException e, OneDriveDataStore dataStore, String parentId,
            OneDriveObjectType objectType, String itemPath, String[] pathParts, String itemName) throws IOException {
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        DriveItem newItem;
        if (e.getResponseCode() == ERROR_CODE_CONFLICT) {
            // file exists, return file data
            newItem = getItemByName(dataStore, parentId, itemPath);
            log.debug("new item " + newItem.name + " was created before");
        } else if (e.getResponseCode() == ERROR_CODE_ITEM_NOT_FOUND) {
            // create parent folders
            for (int i = 0; i < pathParts.length - 1; i++) {
                String objName = pathParts[i];
                DriveItem parentItem = createItem(dataStore, parentId, OneDriveObjectType.DIRECTORY, objName);
                parentId = parentItem.id;
                log.debug("new item " + parentId + " was created");
            }

            // create item (file or folder)
            newItem = createItemInternal(graphClient, parentId, null, objectType, itemName);
            log.debug("new item " + newItem.name + " was created");
        } else {
            // unexpected error
            throw e;
        }
        return newItem;
    }

    private DriveItem createItemInternal(GraphClient graphClient, String parentId, String parentRelativePath,
            OneDriveObjectType objectType, String itemName) {
        DriveItem objectToCreate = new DriveItem();
        objectToCreate.name = itemName;
        if (objectType == OneDriveObjectType.DIRECTORY) {
            objectToCreate.folder = new Folder();
        } else {
            objectToCreate.file = new com.microsoft.graph.models.extensions.File();
        }
        DriveItem newItem;
        if (parentRelativePath == null || parentRelativePath.isEmpty()) {
            newItem = graphClient.getDriveRequestBuilder().items(parentId).children().buildRequest().post(objectToCreate);
        } else {
            newItem = graphClient.getDriveRequestBuilder().items(parentId).itemWithPath(parentRelativePath).children()
                    .buildRequest().post(objectToCreate);
        }
        return newItem;
    }

    /**
     * Delete file or folder
     *
     * @param itemId - id of the deleted item
     * @throws IOException
     */
    public void deleteItem(OneDriveDataStore dataStore, String itemId) {
        log.debug("delete item: " + itemId);
        if (itemId == null || itemId.isEmpty()) {
            return;
        }
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);
        graphClient.getDriveRequestBuilder().items(itemId).buildRequest().delete();

        log.debug("item " + itemId + " was deleted");
    }

    /**
     * get item's content from server
     *
     * @param itemId - id of the item we want to get
     * @return
     * @throws IOException
     */
    public JsonObject getItemData(OneDriveGetConfiguration configuration, String itemId) throws IOException {
        log.debug("get item data: " + itemId);
        if (itemId == null || itemId.isEmpty()) {
            return null;
        }

        OneDriveDataStore dataStore = configuration.getDataSet().getDataStore();
        GraphClient graphClient = graphClientService.getGraphClient(dataStore);

        DriveItem item = getItem(dataStore, itemId);

        boolean isFile = (item.file != null);
        boolean isFolder = (item.folder != null);

        JsonObject res = graphClientService.driveItemToJson(item);

        // get input stream for file
        InputStream inputStream = null;
        if (isFile) {
            inputStream = graphClient.getDriveRequestBuilder().items(itemId).content().buildRequest().get();
        }

        try {
            if (configuration.isStoreFilesLocally()) {
                String parentPath = getItemParentPath(item);
                String fileName = configuration.getStoreDirectory() + "/" + parentPath + "/" + item.name;
                if (isFolder) {
                    File directory = new File(fileName);
                    if (!directory.exists() && !directory.mkdirs()) {
                        throw new RuntimeException("Could not create directory: " + fileName);
                    }
                } else if (isFile) {
                    saveFileLocally(inputStream, item.size, fileName);
                }
            } else if (isFile) {
                String allBytesBase64 = getPayloadBase64(inputStream, item.size);
                res = jsonBuilderFactory.createObjectBuilder(res).add("payload", allBytesBase64).build();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
        }

        log.debug("item " + itemId + " was saved locally");
        return res;
    }

    private String getItemParentPath(DriveItem item) {
        String parentPath = item.parentReference.path;
        final String pathStart = DRIVE_ROOT_PATH;
        if (parentPath.startsWith(pathStart)) {
            parentPath = parentPath.substring(pathStart.length());
        }
        if (parentPath.startsWith("/")) {
            parentPath = parentPath.substring(1);
        }
        return parentPath;
    }

    private void saveFileLocally(InputStream inputStream, Long fileSize, String fileName) throws IOException {
        int totalBytes = 0;
        log.debug("getItemData. fileName: " + fileName);
        File newFile = new File(fileName);
        // create parent dir
        if (!newFile.getParentFile().exists() && !newFile.getParentFile().mkdirs()) {
            throw new RuntimeException("Could not create directory: " + newFile.getParentFile());
        }
        try (OutputStream outputStream = new FileOutputStream(newFile)) {
            int read = 0;
            byte[] bytes = new byte[1024 * 1024];
            while ((read = inputStream.read(bytes)) != -1) {
                totalBytes += read;
                outputStream.write(bytes, 0, read);
                log.debug("progress: " + fileName + ": " + totalBytes + ":" + fileSize);
            }
        }
    }

    private String getPayloadBase64(InputStream inputStream, Long fileSize) throws IOException {
        if (fileSize > Integer.MAX_VALUE) {
            throw new RuntimeException("The file is bigger than " + Integer.MAX_VALUE + " bytes!");
        }
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            int read = 0;
            byte[] bytes = new byte[1024 * 1024];
            while ((read = inputStream.read(bytes)) != -1) {
                outputStream.write(bytes, 0, read);
            }
            byte[] allBytes = outputStream.toByteArray();
            String allBytesBase64 = Base64.getEncoder().encodeToString(allBytes);
            return allBytesBase64;
        }
    }

    /**
     * write data to file onto server
     *
     * @param itemPath - path to file on the server
     * @param inputStream - stream with data
     * @param fileLength - the length of data
     * @return
     * @throws IOException
     */
    public DriveItem putItemData(OneDriveDataStore dataStore, String itemPath, InputStream inputStream, int fileLength)
            throws IOException {
        log.debug("put item data: " + itemPath);

        OneDriveObjectType objectType = OneDriveObjectType.FILE;
        if (inputStream == null) {
            objectType = OneDriveObjectType.DIRECTORY;
        }

        GraphClient graphClient = graphClientService.getGraphClient(dataStore);

        // create item
        DriveItem newItem = createItem(dataStore, null, objectType, itemPath);
        log.debug("new item was created: " + newItem.id);

        if (objectType == OneDriveObjectType.FILE) {
            // upload file content
            UploadSession uploadSession = graphClient.getDriveRequestBuilder().items(newItem.id)
                    .createUploadSession(new DriveItemUploadableProperties()).buildRequest().post();
            ChunkedUploadProvider<DriveItem> provider = new ChunkedUploadProvider<>(uploadSession,
                    graphClientService.getGraphClient(dataStore).getGraphServiceClient(), inputStream, fileLength,
                    DriveItem.class);
            provider.upload(new IProgressCallback<DriveItem>() {

                @Override
                public void progress(long currentBytes, long allBytes) {
                    log.debug("progress: " + itemPath + " -> " + currentBytes + ":" + allBytes);
                }

                @Override
                public void success(DriveItem o) {
                    log.debug("file was uploaded: " + itemPath + ", itemId: " + o.id);
                }

                @Override
                public void failure(ClientException e) {
                    throw e;
                }
            });
        }

        newItem = getItem(dataStore, newItem.id);

        return newItem;
    }
}
