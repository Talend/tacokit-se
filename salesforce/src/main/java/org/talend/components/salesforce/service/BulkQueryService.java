// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.salesforce.service;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.json.JsonBuilderFactory;

import com.sforce.async.AsyncApiException;
import com.sforce.async.AsyncExceptionCode;
import com.sforce.async.BatchInfo;
import com.sforce.async.BatchInfoList;
import com.sforce.async.BatchStateEnum;
import com.sforce.async.BulkConnection;
import com.sforce.async.ConcurrencyMode;
import com.sforce.async.ContentType;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.QueryResultList;
import com.sforce.ws.ConnectionException;

import org.talend.components.salesforce.BulkResultSet;

import lombok.extern.slf4j.Slf4j;

/**
 * This contains process a set of records by creating a job that contains one or more batches. The job specifies which
 * object is being processed and what type of action is being used (query, insert, upsert, update, or delete).
 */

@Slf4j
public class BulkQueryService {

    private final String FILE_ENCODING = "UTF-8";

    private List<BatchInfo> batchInfoList;

    private JobInfo job;

    private List<String> baseFileHeader;

    private ConcurrencyMode concurrencyMode = null;

    private Iterator<String> queryResultIDs = null;

    private boolean safetySwitch = true;

    private int chunkSize;

    private int chunkSleepTime;

    private long jobTimeOut;

    private final Messages messagesI18n;

    private final JsonBuilderFactory jsonBuilderFactory;

    private static final String PK_CHUNKING_HEADER_NAME = "Sforce-Enable-PKChunking";

    private static final String CHUNK_SIZE_PROPERTY_NAME = "chunkSize=";

    private static final int MAX_BATCH_EXECUTION_TIME = 600 * 1000;

    public static final int DEFAULT_CHUNK_SIZE = 100_000;

    public static final int DEFAULT_CHUNK_SLEEP_TIME = 15;

    public static final int MAX_CHUNK_SIZE = 250_000;

    public static final int DEFAULT_JOB_TIME_OUT = 0;
    // Default : no timeout to wait until the job fails or is in success

    private final BulkConnection bulkConnection;

    public BulkQueryService(final BulkConnection bulkConnection, final JsonBuilderFactory jsonBuilderFactory,
            final Messages messages) {
        this.bulkConnection = bulkConnection;
        this.jsonBuilderFactory = jsonBuilderFactory;
        this.messagesI18n = messages;
        this.chunkSize = DEFAULT_CHUNK_SIZE;
        chunkSleepTime = DEFAULT_CHUNK_SLEEP_TIME;
        this.jobTimeOut = DEFAULT_JOB_TIME_OUT;
    }

    /**
     * This is for Bulk connection session renew It can't called automatically with current force-wsc api
     */
    private void renewSession() throws ConnectionException {
        log.debug("renew session bulk connection");
        bulkConnection.getConfig().getSessionRenewer().renewSession(bulkConnection.getConfig());
    }

    /**
     * Creates and executes job for bulk query. Job must be finished in 2 minutes on Salesforce side.<br/>
     * From Salesforce documentation two scenarios are possible here:
     * <ul>
     * <li>simple bulk query. It should have status - {@link BatchStateEnum#Completed}.</li>
     * <li>primary key chunking bulk query. It should return first batch info with status -
     * {@link BatchStateEnum#NotProcessed}.<br/>
     * Other batch info's should have status - {@link BatchStateEnum#Completed}</li>
     * </ul>
     *
     * @param moduleName - input module name.
     * @param queryStatement - to be executed.
     * @throws AsyncApiException
     * @throws InterruptedException
     * @throws ConnectionException
     */
    public void doBulkQuery(String moduleName, String queryStatement)
            throws AsyncApiException, InterruptedException, ConnectionException {
        job = new JobInfo();
        job.setObject(moduleName);
        job.setOperation(OperationEnum.query);
        if (concurrencyMode != null) {
            job.setConcurrencyMode(concurrencyMode);
        }
        job.setContentType(ContentType.CSV);
        job = createJob(job);
        if (job.getId() == null) { // job creation failed
            throw new IllegalStateException("failedBatch" + job);
        }

        ByteArrayInputStream bout = new ByteArrayInputStream(queryStatement.getBytes(StandardCharsets.UTF_8));
        BatchInfo info = createBatchFromStream(job, bout);
        int secToWait = 1;
        int tryCount = 0;
        while (true) {
            log.debug("Awaiting " + secToWait + " seconds for results ...\n" + info);
            Thread.sleep(secToWait * 1000);
            info = getBatchInfo(job.getId(), info.getId());
            if (info.getState() == BatchStateEnum.Completed
                    || (BatchStateEnum.NotProcessed == info.getState() && 0 < chunkSize)) {
                break;
            } else if (info.getState() == BatchStateEnum.Failed) {
                throw new IllegalStateException(info.getStateMessage());
            }
            // after 3 attempt to get the result we multiply the time to wait by 2
            // if secToWait < 120 : don't increase exponentially, no need to sleep more than 128 seconds
            tryCount++;
            if (tryCount % 3 == 0 && secToWait < 120) {
                secToWait = secToWait * 2;
            }

            // The user can specify a global timeout for the job processing to suites some bulk limits :
            // https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_concepts_limits.htm
            if (jobTimeOut > 0) { // if 0, timeout is disabled
                long processingTime = System.currentTimeMillis() - job.getCreatedDate().getTimeInMillis();
                if (processingTime > jobTimeOut) {
                    throw new IllegalStateException("failedBatch: " + info);
                }
            }
        }

        retrieveResultsOfQuery(info);
    }

    public BulkResultSet getQueryResultSet(String resultId) throws AsyncApiException, IOException, ConnectionException {
        final com.csvreader.CsvReader baseFileReader = new com.csvreader.CsvReader(new BufferedReader(
                new InputStreamReader(getQueryResultStream(job.getId(), batchInfoList.get(0).getId(), resultId), FILE_ENCODING)),
                ',');
        baseFileReader.setSafetySwitch(safetySwitch);
        if (baseFileReader.readRecord()) {
            baseFileHeader = Arrays.asList(baseFileReader.getValues());
        }
        return new BulkResultSet(baseFileReader, baseFileHeader, jsonBuilderFactory);
    }

    private JobInfo createJob(JobInfo job) throws AsyncApiException, ConnectionException {
        try {
            if (0 != chunkSize) {
                // Enabling PK chunking by setting header and chunk size.
                bulkConnection.addHeader(PK_CHUNKING_HEADER_NAME, CHUNK_SIZE_PROPERTY_NAME + chunkSize);
            }
            return bulkConnection.createJob(job);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return createJob(job);
            }
            throw sfException;
        } finally {
            if (0 != chunkSize) {
                // Need to disable PK chunking after job was created.
                bulkConnection.addHeader(PK_CHUNKING_HEADER_NAME, Boolean.FALSE.toString());
            }
        }
    }

    private BatchInfo createBatchFromStream(JobInfo job, InputStream input) throws AsyncApiException, ConnectionException {
        try {
            return bulkConnection.createBatchFromStream(job, input);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return createBatchFromStream(job, input);
            }
            throw sfException;
        }
    }

    private BatchInfoList getBatchInfoList(String jobID) throws AsyncApiException, ConnectionException {
        try {
            return bulkConnection.getBatchInfoList(jobID);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return getBatchInfoList(jobID);
            }
            throw sfException;
        }
    }

    private BatchInfo getBatchInfo(String jobID, String batchID) throws AsyncApiException, ConnectionException {
        try {
            return bulkConnection.getBatchInfo(jobID, batchID);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return getBatchInfo(jobID, batchID);
            }
            throw sfException;
        }
    }

    private QueryResultList getQueryResultList(String jobID, String batchID) throws AsyncApiException, ConnectionException {
        try {
            return bulkConnection.getQueryResultList(jobID, batchID);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return getQueryResultList(jobID, batchID);
            }
            throw sfException;
        }
    }

    private InputStream getQueryResultStream(String jobID, String batchID, String resultID)
            throws AsyncApiException, ConnectionException {
        try {
            return bulkConnection.getQueryResultStream(jobID, batchID, resultID);
        } catch (AsyncApiException sfException) {
            if (AsyncExceptionCode.InvalidSessionId.equals(sfException.getExceptionCode())) {
                renewSession();
                return getQueryResultStream(jobID, batchID, resultID);
            }
            throw sfException;
        }
    }

    /**
     * Retrieve resultId(-s) from job batches info.
     * Results will be retrieved only from completed batches.
     *
     * When pk chunking is enabled, we need to go through all batches in the job.
     * More information on Salesforce documentation:
     * https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/
     * asynch_api_code_curl_walkthrough_pk_chunking.htm
     *
     * If some batches were queued or in progress, we must wait till they completed or failed/notprocessed.
     * Quick instructions for primary key chunking flow may be read here:
     * https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_bulk_query_processing.htm
     *
     * @param info - batch info from created job.
     * @throws AsyncApiException
     * @throws ConnectionException
     * @throws InterruptedException
     */
    private void retrieveResultsOfQuery(BatchInfo info) throws AsyncApiException, ConnectionException, InterruptedException {

        if (BatchStateEnum.Completed == info.getState()) {
            QueryResultList list = getQueryResultList(job.getId(), info.getId());
            queryResultIDs = new HashSet<String>(Arrays.asList(list.getResult())).iterator();
            this.batchInfoList = Collections.singletonList(info);
            return;
        }
        BatchInfoList batchInfoList = null;
        Set<String> resultSet = new HashSet<>();
        boolean isInProgress = true;
        while (isInProgress) {
            batchInfoList = getBatchInfoList(job.getId());
            isInProgress = isJobBatchesInProgress(batchInfoList, info);
            if (isInProgress) {
                Thread.sleep(chunkSleepTime);
                long processingTime = System.currentTimeMillis() - job.getCreatedDate().getTimeInMillis();
                if (processingTime > MAX_BATCH_EXECUTION_TIME) {
                    // Break processing and return processed data if any batch was processed.
                    log.warn(messagesI18n.warnBatchTimeout());
                    break;
                }
            }
        }
        for (BatchInfo batch : batchInfoList.getBatchInfo()) {
            if (batch.getId().equals(info.getId())) {
                continue;
            }
            resultSet.addAll(Arrays.asList(getQueryResultList(job.getId(), batch.getId()).getResult()));
        }

        queryResultIDs = resultSet.iterator();
        this.batchInfoList = Arrays.asList(batchInfoList.getBatchInfo());
    }

    /**
     * Checks if job batch infos were processed correctly. Only if all batches were {@link BatchStateEnum#Completed} are
     * acceptable.<br/>
     * If any of batches returns {@link BatchStateEnum#Failed} or {@link BatchStateEnum#NotProcessed} - throws an exception.
     *
     * @param batchInfoList - batch infos related to the specific job.
     * @param info - batch info for query batch.
     * @return true - if job is not processed fully, otherwise - false.
     */
    private boolean isJobBatchesInProgress(BatchInfoList batchInfoList, BatchInfo info) {
        for (BatchInfo batch : batchInfoList.getBatchInfo()) {
            if (batch.getId().equals(info.getId())) {
                continue;
            }

            /*
             * More details about every batch state can be found here:
             * https://developer.salesforce.com/docs/atlas.en-us.api_asynch.meta/api_asynch/asynch_api_batches_interpret_status.
             * htm
             */
            switch (batch.getState()) {
            case Completed:
                break;
            case NotProcessed:
                /*
                 * If batch was not processed we should abort further execution.
                 * From official documentation:
                 * The batch won’t be processed. This state is assigned when a job is aborted while the batch is queued.
                 */
            case Failed:
                throw new IllegalStateException("ERROR_IN_BULK_QUERY_PROCESSING: " + batch.getStateMessage());
            case Queued:
            case InProgress:
                return true;
            }
        }
        return false;
    }

    public String nextResultId() {
        String resultId = null;
        if (queryResultIDs != null && queryResultIDs.hasNext()) {
            resultId = queryResultIDs.next();
        }
        return resultId;
    }
}
