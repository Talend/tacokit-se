package org.talend.components.onedrive.sources.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.io.IOException;
import java.util.Iterator;

public class DriveItemWrapper implements Iterator<DriveItem> {

    private DriveItem driveItem;

    private Iterator<DriveItem> pages;

    public DriveItemWrapper(OneDriveDataStore dataStore, OneDriveHttpClientService oneDriveHttpClientService, DriveItem driveItem)
            throws IOException {
        this.driveItem = driveItem;
        if (driveItem != null)
            pages = new PageIterator(new PageWrapper(dataStore, oneDriveHttpClientService,
                    oneDriveHttpClientService.getItemChildren(dataStore, driveItem)));
    }

    public DriveItem getDriveItem() {
        return driveItem;
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("has next is unsupported");
    }

    @Override
    public DriveItem next() {
        return pages == null ? null : pages.next();
    }
}
