package org.talend.components.onedrive.sources.list.iterator;

import com.microsoft.graph.models.extensions.DriveItem;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.service.http.OneDriveHttpClientService;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class DriveItemIterator implements Iterator<DriveItem> {

    private Iterator<DriveItem> driveItemIterator;

    private DriveItemWrapper driveItemWrapper;

    private OneDriveHttpClientService oneDriveHttpClientService;

    private OneDriveDataStore dataStore;

    public DriveItemIterator(OneDriveDataStore dataStore, OneDriveHttpClientService oneDriveHttpClientService,
            List<DriveItem> items) throws IOException {
        this.oneDriveHttpClientService = oneDriveHttpClientService;
        this.dataStore = dataStore;
        if (items == null || items.isEmpty()) {
            driveItemIterator = null;
        }

        driveItemIterator = items.iterator();
        if (driveItemIterator.hasNext()) {
            driveItemWrapper = new DriveItemWrapper(dataStore, oneDriveHttpClientService, driveItemIterator.next());
        }
    }

    @Override
    public boolean hasNext() {
        throw new UnsupportedOperationException("has next is unsupported");
    }

    @Override
    public DriveItem next() {
        if (driveItemIterator == null) {
            return null;
        }

        if (driveItemWrapper == null) {
            return null;
        }

        DriveItem res = driveItemWrapper.next();
        if (res == null) {
            res = driveItemWrapper.getDriveItem();
            if (driveItemIterator.hasNext()) {
                try {
                    driveItemWrapper = new DriveItemWrapper(dataStore, oneDriveHttpClientService, driveItemIterator.next());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else {
                driveItemWrapper = null;
            }
        }
        return res;
    }
}
