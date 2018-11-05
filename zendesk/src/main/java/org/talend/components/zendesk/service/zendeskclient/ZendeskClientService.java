package org.talend.components.zendesk.service.zendeskclient;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.zendesk.common.AuthenticationType;
import org.talend.components.zendesk.common.ZendeskDataStore;
import org.talend.sdk.component.api.service.Service;
import org.zendesk.client.v2.Zendesk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class ZendeskClientService {

    private Map<ZendeskDataStore, Zendesk> zendeskClients = new ConcurrentHashMap<>();

    public Zendesk getZendeskClientWrapper(ZendeskDataStore zendeskDataStore) {
        return zendeskClients.computeIfAbsent(zendeskDataStore, key -> createClient(zendeskDataStore));
    }

    private Zendesk createClient(ZendeskDataStore dataStore) {
        if (dataStore.getAuthenticationType() == AuthenticationType.LOGIN_PASSWORD) {
            return new Zendesk.Builder(dataStore.getServerUrl())
                    .setUsername(dataStore.getAuthenticationLoginPasswordConfiguration().getAuthenticationLogin())
                    .setPassword(dataStore.getAuthenticationLoginPasswordConfiguration().getAuthenticationPassword()).build();
        } else if (dataStore.getAuthenticationType() == AuthenticationType.API_TOKEN) {
            return new Zendesk.Builder(dataStore.getServerUrl())
                    .setUsername(dataStore.getAuthenticationApiTokenConfiguration().getAuthenticationLogin())
                    .setToken(dataStore.getAuthenticationApiTokenConfiguration().getApiToken()).build();
        } else {
            throw new UnsupportedOperationException("Unknown authentication type");
        }
    }
}
