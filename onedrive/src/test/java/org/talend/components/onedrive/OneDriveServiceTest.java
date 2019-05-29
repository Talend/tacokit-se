package org.talend.components.onedrive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.onedrive.common.OneDriveDataSet;
import org.talend.components.onedrive.service.OneDriveService;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@WithComponents("org.talend.components.onedrive")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OneDriveServiceTest {

    @Service
    private OneDriveService oneDriveService;

    @Test
    @DisplayName("Schema discovery List")
    void schemaDiscoveryListTest() {
        log.info("Integration test 'Schema discovery list' start ");
        OneDriveDataSet dataSet = new OneDriveDataSet();
        Schema schema = oneDriveService.guessTableSchemaList(dataSet);
        assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id", "createdDateTime", "eTag", "lastModifiedDateTime", "name", "webUrl", "cTag",
                        "size", "createdBy", "lastModifiedBy", "parentReference", "fileSystemInfo", "folder", "file", "root")));
    }

    @Test
    @DisplayName("Schema discovery Delete")
    void schemaDiscoveryDeleteTest() {
        log.info("Integration test 'Schema discovery delete' start ");
        OneDriveDataSet dataSet = new OneDriveDataSet();
        Schema schema = oneDriveService.guessTableSchemaDelete(dataSet);
        assertTrue(schema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList())
                .containsAll(Arrays.asList("id")));
    }
}
