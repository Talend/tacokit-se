package org.talend.components.onedrive.sources.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.io.IOException;
import java.util.Iterator;

public class PageWrapper implements Iterator<DriveItem> {

    private IDriveItemCollectionPage page;

    private Iterator<DriveItem> items;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private OneDriveDataStore dataStore;

    public PageWrapper(OneDriveDataStore dataStore, OneDriveHttpClientService oneDriveHttpClientService,
            IDriveItemCollectionPage page) throws IOException {
        this.page = page;
        this.dataStore = dataStore;
        this.oneDriveHttpClientService = oneDriveHttpClientService;

        if (page != null)
            items = new DriveItemIterator(dataStore, oneDriveHttpClientService, page.getCurrentPage());

    }

    public PageWrapper getNextPageWrapper() throws IOException {
        if (page == null || page.getNextPage() == null) {
            return null;
        }
        return new PageWrapper(dataStore, oneDriveHttpClientService, page.getNextPage().buildRequest().get());
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("has next is unsupported");
    }

    @Override
    public DriveItem next() {
        return items == null ? null : items.next();
    }
}
