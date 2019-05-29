package org.talend.components.onedrive.helpers.authhandlers;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.onedrive.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.onedrive.common.OneDriveDataStore;
import org.talend.components.onedrive.common.UnknownAuthenticationTypeException;
import org.talend.components.onedrive.service.http.OneDriveAuthHttpClientService;
import org.talend.sdk.component.api.service.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class AuthorizationHandlerLoginPassword implements AuthorizationHandler {

    private static final String BEARER = "Bearer ";

    private static Map<AuthenticationLoginPasswordConfiguration, String> cachedTokens = new ConcurrentHashMap<>();

    @Service
    private OneDriveAuthHttpClientService oneDriveAuthHttpClientService;

    public void clearTokenCache(AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration) {
        cachedTokens.remove(authenticationLoginPasswordConfiguration);
    }

    @Override
    public String getAuthorization(OneDriveDataStore oneDriveDataStore) {
        String accessToken = getCachedToken(oneDriveDataStore);
        return BEARER + accessToken;
    }

    String getCachedToken(OneDriveDataStore oneDriveDataStore) throws UnknownAuthenticationTypeException {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) oneDriveDataStore
                .getAuthSettings();
        return cachedTokens.computeIfAbsent(authSettings, key -> getToken(oneDriveDataStore));
    }

    private String getToken(OneDriveDataStore oneDriveDataStore) {
        AuthenticationLoginPasswordConfiguration authSettings = (AuthenticationLoginPasswordConfiguration) oneDriveDataStore
                .getAuthSettings();
        String login = authSettings.getAuthenticationLogin();
        String password = authSettings.getAuthenticationPassword();
        String tenantId = oneDriveDataStore.getTenantId();
        String applicationId = oneDriveDataStore.getApplicationId();

        return oneDriveAuthHttpClientService.getToken(tenantId, applicationId, login, password);
    }
}
