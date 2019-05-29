package org.talend.components.onedrive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.TestInstance;
import org.talend.sdk.component.junit5.WithComponents;

@Slf4j
@WithComponents("org.talend.components.onedrive")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class OneDriveTest {

    // @Test
    // public void testConnection() throws UnknownAuthenticationTypeException {
    // AuthenticationLoginPasswordConfiguration authenticationLoginPasswordConfiguration = new
    // AuthenticationLoginPasswordConfiguration();
    // OneDriveDataStore oneDriveDataStore;
    // oneDriveDataStore = new OneDriveDataStore(null, null, AuthenticationType.LOGIN_PASSWORD,
    // authenticationLoginPasswordConfiguration);
    // assertEquals(authenticationLoginPasswordConfiguration, oneDriveDataStore.getAuthSettings());
    // }

}
