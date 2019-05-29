package org.talend.components.onedrive.sources.list;

import org.talend.components.onedrive.service.graphclient.GraphClientService;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "onedrive_list")
@PartitionMapper(name = "List")
@Documentation("List mapper class")
public class OneDriveListMapper implements Serializable {

    private final OneDriveListConfiguration configuration;

    private final OneDriveHttpClientService oneDriveHttpClientService;

    private final GraphClientService graphClientService;

    public OneDriveListMapper(@Option("configuration") final OneDriveListConfiguration configuration,
            OneDriveAuthHttpClientService oneDriveAuthHttpClientService, OneDriveHttpClientService oneDriveHttpClientService,
            GraphClientService graphClientService) {
        this.configuration = configuration;
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.graphClientService = graphClientService;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<OneDriveListMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter(name = "List")
    public OneDriveListSource createWorker() {
        return new OneDriveListSource(configuration, oneDriveHttpClientService, graphClientService);
    }
}