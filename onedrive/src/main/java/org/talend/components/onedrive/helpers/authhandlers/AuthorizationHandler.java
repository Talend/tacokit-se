package org.talend.components.onedrive.helpers.authhandlers;

import org.talend.components.onedrive.common.OneDriveDataStore;

public interface AuthorizationHandler {

    String getAuthorization(OneDriveDataStore oneDriveDataStore);
}
