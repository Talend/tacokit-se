/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.netsuite.runtime.client;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.ws.BindingProvider;

import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.headers.Header;
import org.apache.cxf.transport.http.HTTPConduit;
import org.apache.cxf.transports.http.configuration.HTTPClientPolicy;
import org.talend.components.netsuite.runtime.NetSuiteErrorCode;
import org.talend.components.netsuite.runtime.client.search.SearchQuery;
import org.talend.components.netsuite.runtime.model.BasicMetaData;
import org.talend.components.netsuite.service.Messages;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class NetSuiteClientService<PortT> {

    private static final String APPLICATION_INFO = "applicationInfo";

    private static final String PREFERENCES = "preferences";

    private static final String SEARCH_PREFERENCES = "searchPreferences";

    private static final long DEFAULT_CONNECTION_TIMEOUT = TimeUnit.SECONDS.toMillis(60);

    private static final long DEFAULT_RECEIVE_TIMEOUT = TimeUnit.SECONDS.toMillis(180);

    private static final int DEFAULT_SEARCH_PAGE_SIZE = 100;

    private static final String JAXB_CONTEXT_OLD = "com.sun.xml.bind.v2.runtime.JAXBContextImpl";

    private static final String JAXB_CONTEXT = "com.sun.xml.internal.bind.v2.runtime.JAXBContextImpl";

    public Messages i18n;

    private boolean isLoggedIn = false;

    protected String endpointUrl;

    protected NetSuiteCredentials credentials;

    protected NsTokenPassport tokenPassport;

    protected NsSearchPreferences searchPreferences;

    protected NsPreferences preferences;

    /** Used for synchronization of access to NetSuite port. */
    protected ReentrantLock lock = new ReentrantLock();

    /** Specifies whether logging of SOAP messages is enabled. Intended for test/debug purposes. */
    protected boolean messageLoggingEnabled = false;

    /** Web Service connection timeout, in milliseconds. */
    protected long connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;

    /** Web Service response receiving timeout, in milliseconds. */
    protected long receiveTimeout = DEFAULT_RECEIVE_TIMEOUT;

    /** Number of retries for an operation. */
    protected int retryCount = 3;

    /** Number of retries before (re-)login. */
    protected int retriesBeforeLogin = 2;

    /** Interval between retries. */
    protected int retryInterval = 5;

    /** Size of search result page. */
    protected int searchPageSize = DEFAULT_SEARCH_PAGE_SIZE;

    /** Specifies whether to return search columns. */
    protected boolean returnSearchColumns = false;

    /** Specifies whether to treat warnings as errors. */
    protected boolean treatWarningsAsErrors = false;

    /** Specifies whether to disable validation for mandatory custom fields. */
    protected boolean disableMandatoryCustomFieldValidation = false;

    /** Specifies whether to use request level credentials. */
    protected boolean useRequestLevelCredentials = false;

    /** Specifies whether to use request token based authentication. */
    protected boolean useTokens = false;

    protected PortAdapter<PortT> portAdapter;

    protected PortT port;

    /** Source of meta data. */
    protected MetaDataSource metaDataSource;

    protected NetSuiteClientService() {
        String prefix = null;
        try {
            prefix = Class.forName(JAXB_CONTEXT_OLD).getName();
        } catch (ClassNotFoundException e) {
            try {
                prefix = Class.forName(JAXB_CONTEXT).getName();
            } catch (ClassNotFoundException e1) {
                // ignore
            }
        }
        if (prefix != null) {
            // Disable eager initialization of JAXBContext
            System.setProperty(prefix + ".fastBoot", "true");
        }
    }

    public void setBodyFieldsOnly(boolean bodyFieldsOnly) {
        searchPreferences.setBodyFieldsOnly(bodyFieldsOnly);
        Object searchPreferencesObject = createNativeSearchPreferences(searchPreferences);
        try {
            Header searchPreferencesHeader = new Header(new QName(getPlatformMessageNamespaceUri(), SEARCH_PREFERENCES),
                    searchPreferencesObject, JAXBDataBindingCache.getInstance().getBinding(searchPreferencesObject.getClass()));
            setHeader(port, searchPreferencesHeader);
        } catch (JAXBException e) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), i18n.bindingError(), e);
        }
    }

    /**
     * Log in to NetSuite.
     *
     * @throws NetSuiteException if an error occurs during logging in
     */
    public void login() throws NetSuiteException {
        lock.lock();
        try {
            relogin();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Create new search query object using given meta data source.
     *
     * @param metaDataSource meta data source
     * @return search query object
     */
    public SearchQuery<?, ?> newSearch(MetaDataSource metaDataSource, String recordTypeName, boolean customizationEnabled) {
        return new SearchQuery<>(this, metaDataSource, i18n, recordTypeName, customizationEnabled);
    }

    /**
     * Search records.
     *
     * <p>
     * Retrieval of search results uses pagination. To retrieve next page use
     * {@link #searchMoreWithId(String, int)} method.
     *
     * @param searchRecord search record to be sent to NetSuite
     * @param <RecT> type of record data object
     * @param <SearchT> type of search record data object
     * @return search result wrapper object
     */
    public <RecT, SearchT> NsSearchResult<RecT> search(final SearchT searchRecord) {
        return Optional.ofNullable(searchRecord)
                .map(list -> execute(port -> (NsSearchResult<RecT>) portAdapter.search(port, list)))
                .orElse(new NsSearchResult<>(new NsStatus(false, Collections.emptyList())));
    }

    /**
     * Retrieve search results page by search ID and page index.
     *
     * @param searchId identifier of search
     * @param pageIndex page index
     * @param <RecT> type of record data object
     * @return search result wrapper object
     */
    public <RecT> NsSearchResult<RecT> searchMoreWithId(final String searchId, final int pageIndex) {
        return Optional.ofNullable(searchId)
                .map(id -> execute(port -> (NsSearchResult<RecT>) portAdapter.searchMoreWithId(port, id, pageIndex)))
                .orElse(new NsSearchResult<>(new NsStatus(false, Collections.emptyList())));
    }

    /**
     * Add records.
     *
     * @param records list of record data objects to be sent to NetSuite
     * @param <RecT> type of record data object
     * @return list of write response wrapper objects
     * @throws NetSuiteException if an error occurs during performing of operation
     */
    public <RecT> List<NsWriteResponse<?>> addList(final List<RecT> records) throws NetSuiteException {
        return Optional.ofNullable(records).filter(list -> !list.isEmpty())
                .map(list -> execute(port -> portAdapter.addList(port, list))).orElse(Collections.emptyList());
    }

    /**
     * Update records.
     *
     * @param records list of record data objects to be sent to NetSuite
     * @param <RecT> type of record data object
     * @return list of write response wrapper objects
     */
    public <RecT> List<NsWriteResponse<?>> updateList(final List<RecT> records) {
        return Optional.ofNullable(records).filter(list -> !list.isEmpty())
                .map(list -> execute(port -> portAdapter.updateList(port, list))).orElse(Collections.emptyList());
    }

    /**
     * Upsert records.
     *
     * @param records list of record data objects to be sent to NetSuite
     * @param <RecT> type of record data object
     * @param <RefT> type of record ref data object
     * @return list of write response wrapper objects
     */
    public <RecT, RefT> List<NsWriteResponse<?>> upsertList(final List<RecT> records) {
        return Optional.ofNullable(records).filter(list -> !list.isEmpty())
                .map(list -> execute(port -> portAdapter.upsertList(port, list))).orElse(Collections.emptyList());
    }

    /**
     * Delete records.
     *
     * @param refs list of record ref data objects to be sent to NetSuite
     * @param <RefT> type of record ref data object
     * @return list of write response wrapper objects
     */
    public <RefT> List<NsWriteResponse<?>> deleteList(final List<?> refs) {
        return Optional.ofNullable(refs).filter(list -> !list.isEmpty())
                .map(list -> execute(port -> portAdapter.deleteList(port, list))).orElse(Collections.emptyList());
    }

    /**
     * Execute an operation that use NetSuite web service port.
     *
     * @param op operation to be executed
     * @param <R> type of operation result
     * @return result of operation
     */
    public <R> R execute(PortOperation<R, PortT> op) {
        boolean execUsingToken = (tokenPassport != null);
        boolean execUsingRequestLevelCreds = !execUsingToken && useRequestLevelCredentials;
        boolean execUsingLogin = !execUsingToken && !useRequestLevelCredentials;

        lock.lock();
        try {
            if (execUsingToken)
                refreshTokenSignature();
            else if (execUsingRequestLevelCreds)
                relogin();
            else if (execUsingLogin)
                login(false);

            R result = null;
            for (int i = 0; i < getRetryCount(); i++) {
                try {
                    result = op.execute(port);
                    break;
                } catch (Exception e) {
                    if (errorCanBeWorkedAround(e)) {
                        log.debug("Attempting workaround, retrying ({})", (i + 1));
                        waitForRetryInterval();
                        if (execUsingLogin) {
                            if (errorRequiresNewLogin(e) || i >= getRetriesBeforeLogin() - 1) {
                                log.debug("Re-logging in ({})", (i + 1));
                                relogin();
                            }
                        }
                    } else {
                        throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), e.getMessage(), e);
                    }
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Execute an operation within client lock.
     *
     * @param func operation to be executed
     * @param param parameter object
     * @param <T> type of parameter
     * @param <R> type of result
     * @return result of execution
     */
    public <T, R> R executeWithLock(Function<T, R> func, T param) {
        lock.lock();
        try {
            return func.apply(param);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get basic meta data used by this client.
     *
     * @return basic meta data
     */
    public abstract BasicMetaData getBasicMetaData();

    /**
     * Create new instance of customization meta data source.
     *
     * @return customization meta data source
     */
    public abstract CustomMetaDataSource createDefaultCustomMetaDataSource();

    /**
     * Set a SOAP header to be sent to NetSuite in request
     *
     * @param port port
     * @param header header to be set
     */
    protected void setHeader(PortT port, Header header) {
        BindingProvider provider = (BindingProvider) port;
        Map<String, Object> requestContext = provider.getRequestContext();
        List<Header> list = (List<Header>) requestContext.get(Header.HEADER_LIST);
        if (list == null) {
            list = new ArrayList<>();
            requestContext.put(Header.HEADER_LIST, list);
        }
        removeHeader(list, header.getName());
        list.add(header);
    }

    /**
     * Remove a SOAP header from header list to be sent to NetSuite
     *
     * @param port port
     * @param name name identifying a header
     */
    protected void removeHeader(PortT port, QName name) {
        BindingProvider provider = (BindingProvider) port;
        Map<String, Object> requestContext = provider.getRequestContext();
        List<Header> list = (List<Header>) requestContext.get(Header.HEADER_LIST);
        removeHeader(list, name);
    }

    /**
     * Remove a SOAP header from given header list.
     *
     * @param list header list
     * @param name name identifying a header
     */
    private void removeHeader(List<Header> list, QName name) {
        if (list != null) {
            Iterator<Header> headerIterator = list.iterator();
            while (headerIterator.hasNext()) {
                Header header = headerIterator.next();
                if (header.getName().equals(name)) {
                    headerIterator.remove();
                }
            }
        }
    }

    /**
     * Forcibly re-log in the client.
     *
     */
    private void relogin() {
        login(true);
    }

    /**
     * Log in the client.
     *
     * @param relogin specifies whether the client should be forcibly re-logged in
     */
    private void login(boolean relogin) {
        if (relogin) {
            isLoggedIn = false;
        }
        if (isLoggedIn) {
            return;
        }

        if (port != null && credentials != null) {
            try {
                doLogout();
            } catch (Exception e) {
            }
        }

        doLogin();

        NsSearchPreferences searchPreferences = new NsSearchPreferences();
        searchPreferences.setPageSize(searchPageSize);
        searchPreferences.setReturnSearchColumns(Boolean.valueOf(returnSearchColumns));

        this.searchPreferences = searchPreferences;

        NsPreferences preferences = new NsPreferences();
        preferences.setDisableMandatoryCustomFieldValidation(disableMandatoryCustomFieldValidation);
        preferences.setWarningAsError(treatWarningsAsErrors);

        this.preferences = preferences;

        setPreferences(port, preferences, searchPreferences);

        isLoggedIn = true;
    }

    /**
     * Perform 'log out' operation.
     *
     */
    protected abstract void doLogout();

    /**
     * Perform 'log in' operation.
     *
     */
    protected abstract void doLogin();

    protected void waitForRetryInterval() {
        try {
            Thread.sleep(getRetryInterval() * 1000);
        } catch (InterruptedException e) {

        }
    }

    /**
     * Check whether given error can be worked around by retrying.
     *
     * @param t error to be checked
     * @return {@code true} if the error can be worked around, {@code false} otherwise
     */
    protected abstract boolean errorCanBeWorkedAround(Throwable t);

    /**
     * Check whether given error can be requires new log-in.
     *
     * @param t error to be checked
     * @return {@code true} if the error requires new log-in, {@code false} otherwise
     */
    protected abstract boolean errorRequiresNewLogin(Throwable t);

    /**
     * Set preferences for given port.
     *
     * @param port port which to set preferences for
     * @param nsPreferences general preferences
     * @param nsSearchPreferences search preferences
     */
    protected void setPreferences(PortT port, NsPreferences nsPreferences, NsSearchPreferences nsSearchPreferences) {

        Object searchPreferences = createNativeSearchPreferences(nsSearchPreferences);
        Object preferences = createNativePreferences(nsPreferences);
        try {
            Header searchPreferencesHeader = new Header(new QName(getPlatformMessageNamespaceUri(), SEARCH_PREFERENCES),
                    searchPreferences, JAXBDataBindingCache.getInstance().getBinding(searchPreferences.getClass())
            // new JAXBDataBinding(searchPreferences.getClass())
            );
            Header preferencesHeader = new Header(new QName(getPlatformMessageNamespaceUri(), PREFERENCES), preferences,
                    JAXBDataBindingCache.getInstance().getBinding(preferences.getClass())
            // new JAXBDataBinding(preferences.getClass())
            );
            setHeader(port, preferencesHeader);
            setHeader(port, searchPreferencesHeader);
        } catch (JAXBException e) {
            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), i18n.bindingError(), e);
        }
    }

    /**
     * Set log-in specific SOAP headers for given port.
     *
     * @param port port
     */
    protected void setLoginHeaders(PortT port) {
        Optional.ofNullable(credentials.getApplicationId()).filter(((Predicate<String>) String::isEmpty).negate())
                .ifPresent((appId) -> Optional.ofNullable(createNativeApplicationInfo(credentials)).ifPresent(applicationInfo -> {
                    try {
                        Header appInfoHeader = new Header(new QName(getPlatformMessageNamespaceUri(), APPLICATION_INFO),
                                applicationInfo, JAXBDataBindingCache.getInstance().getBinding(applicationInfo.getClass())
                        // new JAXBDataBinding(applicationInfo.getClass())
                        );
                        setHeader(port, appInfoHeader);
                    } catch (JAXBException e) {
                        throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.INTERNAL_ERROR), i18n.bindingError(),
                                e);
                    }
                }));
    }

    /**
     * Remove log-in specific SOAP headers for given port.
     *
     * @param port port
     */
    protected void removeLoginHeaders(PortT port) throws NetSuiteException {
        removeHeader(port, new QName(getPlatformMessageNamespaceUri(), APPLICATION_INFO));
    }

    /**
     * Set HTTP client policy for given port.
     *
     * @param port port
     */
    protected void setHttpClientPolicy(PortT port) {
        HTTPClientPolicy httpClientPolicy = new HTTPClientPolicy();
        httpClientPolicy.setConnectionTimeout(connectionTimeout);
        httpClientPolicy.setReceiveTimeout(receiveTimeout);
        setHttpClientPolicy(port, httpClientPolicy);
    }

    protected void setHttpClientPolicy(PortT port, HTTPClientPolicy httpClientPolicy) {
        Client proxy = ClientProxy.getClient(port);
        HTTPConduit conduit = (HTTPConduit) proxy.getConduit();
        conduit.setClient(httpClientPolicy);
    }

    /**
     * Get URI for 'platform message' namespace.
     *
     * @return namespace URI
     */
    protected abstract String getPlatformMessageNamespaceUri();

    /**
     * Create instance of NetSuite's {@code Preferences} native data object.
     *
     * @param nsPreferences source preferences data object
     * @param <T> type of native data object
     * @return {@code Preferences} data object
     */
    protected abstract <T> T createNativePreferences(NsPreferences nsPreferences);

    /**
     * Create instance of NetSuite's {@code SearchPreferences} native data object.
     *
     * @param nsSearchPreferences source search preferences data object
     * @param <T> type of native data object
     * @return {@code SearchPreferences} data object
     */
    protected abstract <T> T createNativeSearchPreferences(NsSearchPreferences nsSearchPreferences);

    /**
     * Create instance of NetSuite's {@code ApplicationInfo} native data object.
     *
     * @param nsCredentials credentials data object
     * @param <T> type of native data object
     * @return {@code ApplicationInfo} data object
     */
    protected abstract <T> T createNativeApplicationInfo(NetSuiteCredentials nsCredentials);

    /**
     * Create instance of NetSuite's {@code Passport} native data object.
     *
     * @param nsCredentials credentials data object
     * @param <T> type of native data object
     * @return {@code Passport} data object
     */
    protected abstract <T> T createNativePassport(NetSuiteCredentials nsCredentials);

    protected abstract <T> T createNativeTokenPassport();

    protected abstract void refreshTokenSignature();

    /**
     * Get instance of NetSuite web service port implementation.
     *
     * @param defaultEndpointUrl default URL of NetSuite endpoint
     * @param account NetSuite account number
     * @return port
     */
    protected abstract void setNetSuitePort(String defaultEndpointUrl, String account);

    /**
     * Check 'log-in' operation status and throw {@link NetSuiteException} if status indicates that
     * an error occurred or exception message is present.
     *
     * @param status status object to be checked, if present
     * @param exceptionMessage exception message, if present
     */
    protected void checkLoginError(NsStatus status, String exceptionMessage) {
        if (status == null || !status.isSuccess()) {
            StringBuilder sb = new StringBuilder();
            if (status != null && status.getDetails().size() > 0) {
                NsStatus.Detail detail = status.getDetails().get(0);
                sb.append(detail.getCode()).append(" ").append(detail.getMessage());
            } else if (exceptionMessage != null) {
                sb.append(exceptionMessage);
            }

            throw new NetSuiteException(new NetSuiteErrorCode(NetSuiteErrorCode.CLIENT_ERROR), i18n.failedToLogin(sb.toString()));
        }
    }

    /**
     * Operation that requires NetSuite port.
     *
     * @param <R> type of operation result
     * @param <PortT> type of NetSuite port implementation
     */
    public interface PortOperation<R, PortT> {

        R execute(PortT port) throws Exception;
    }

    /**
     * Check status of an operation and throw {@link NetSuiteException} if status indicates that
     * an error occurred.
     *
     * @param status status object to be checked
     */
    public static void checkError(NsStatus status) {
        if (!status.getDetails().isEmpty()) {
            NsStatus.Detail detail = status.getDetails().get(0);
            if (detail.getType() == NsStatus.Type.ERROR) {
                throw new NetSuiteException(new NetSuiteErrorCode(detail.getCode()), detail.getMessage());
            }
        }
    }

    protected interface PortAdapter<PortT> {

        /**
         * Search records.
         *
         * <p>
         * Retrieval of search results uses pagination. To retrieve next page use
         * {@link #searchMoreWithId(String, int)} method.
         *
         * @param searchRecord search record to be sent to NetSuite
         * @param <RecT> type of record data object
         * @param <SearchT> type of search record data object
         * @return search result wrapper object
         * @throws Exception if an error occurs during performing of operation
         */
        <RecT, SearchT> NsSearchResult<RecT> search(final PortT port, final SearchT searchRecord) throws Exception;

        /**
         * Retrieve search results page by search ID and page index.
         *
         * @param searchId identifier of search
         * @param pageIndex page index
         * @param <RecT> type of record data object
         * @return search result wrapper object
         * @throws Exception if an error occurs during performing of operation
         */
        <RecT> NsSearchResult<RecT> searchMoreWithId(final PortT port, final String searchId, final int pageIndex)
                throws Exception;

        /**
         * Add records.
         *
         * @param records list of record data objects to be sent to NetSuite
         * @param <RecT> type of record data object
         * @param <RefT> type of record ref data object
         * @return list of write response wrapper objects
         * @throws Exception if an error occurs during performing of operation
         */
        <RecT, RefT> List<NsWriteResponse<?>> addList(final PortT port, final List<RecT> records) throws Exception;

        /**
         * Update records.
         *
         * @param records list of record data objects to be sent to NetSuite
         * @param <RecT> type of record data object
         * @param <RefT> type of record ref data object
         * @return list of write response wrapper objects
         * @throws Exception if an error occurs during performing of operation
         */
        <RecT, RefT> List<NsWriteResponse<?>> updateList(final PortT port, final List<RecT> records) throws Exception;

        /**
         * Upsert records.
         *
         * @param records list of record data objects to be sent to NetSuite
         * @param <RecT> type of record data object
         * @param <RefT> type of record ref data object
         * @return list of write response wrapper objects
         * @throws Exception if an error occurs during performing of operation
         */
        <RecT, RefT> List<NsWriteResponse<?>> upsertList(final PortT port, final List<RecT> records) throws Exception;

        /**
         * Delete records.
         *
         * @param refs list of record ref data objects to be sent to NetSuite
         * @param <RefT> type of record ref data object
         * @return list of write response wrapper objects
         * @throws Exception if an error occurs during performing of operation
         */
        <RefT> List<NsWriteResponse<?>> deleteList(final PortT port, final List<?> refs) throws Exception;

    }
}