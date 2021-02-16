/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.service;

public enum ApiErrors {
    AccountIsDisabled("The specified account is disabled."),
    AuthorizationFailure("This request is not authorized to perform this operation."),
    ConditionNotMet("The condition specified using HTTP conditional header(s) is not met."),
    ContentLengthMustBeZero("The Content-Length request header must be zero."),
    DestinationPathIsBeingDeleted("The specified destination path is marked to be deleted."),
    DirectoryNotEmpty("The recursive query parameter value must be true to delete a non-empty directory."),
    FilesystemAlreadyExists("The specified filesystem already exists."),
    FilesystemBeingDeleted("The specified filesystem is being deleted."),
    FilesystemNotFound("The specified filesystem does not exist."),
    InsufficientAccountPermissions("The account being accessed does not have sufficient permissions to execute this operation."),
    InternalError("The server encountered an internal error. Please retry the request."),
    InvalidAuthenticationInfo(
            "Authentication information is not given in the correct format. Check the value of Authorization header."),
    InvalidDestinationPath(
            "The specified path, or an element of the path, exists and its resource type is invalid for this operation."),
    InvalidFlushOperation(
            "The resource was created or modified by the Blob Service API and cannot be written to by the Data Lake Storage Service API."),
    InvalidFlushPosition(
            "The uploaded data is not contiguous or the position query parameter value is not equal to the length of the file after appending the uploaded data."),
    InvalidHeaderValue("The value for one of the HTTP headers is not in the correct format."),
    InvalidHttpVerb("The HTTP verb specified is invalid - it is not recognized by the server."),
    InvalidInput("One of the request inputs is not valid."),
    InvalidPropertyName("The property name contains invalid characters."),
    InvalidQueryParameterValue("Value for one of the query parameters specified in the request URI is invalid."),
    InvalidRange("The range specified is invalid for the current size of the resource."),
    InvalidRenameSourcePath(
            "The source directory cannot be the same as the destination directory, nor can the destination be a subdirectory of the source directory."),
    InvalidResourceName("The specified resource name contains invalid characters."),
    InvalidSourceOrDestinationResourceType("The source and destination resource type must be identical."),
    InvalidSourceUri("The source URI is invalid."),
    InvalidUri("The request URI is invalid."),
    LeaseAlreadyPresent("There is already a lease present."),
    LeaseIdMismatch("The lease ID specified did not match the lease ID for the resource."),
    LeaseIdMismatchWithLeaseOperation(
            "The lease ID specified did not match the lease ID for the resource with the specified lease operation."),
    LeaseIdMissing("There is currently a lease on the resource and no lease ID was specified in the request."),
    LeaseIsAlreadyBroken("The lease has already been broken and cannot be broken again."),
    LeaseIsBreakingAndCannotBeAcquired(
            "The lease ID matched, but the lease is currently in breaking state and cannot be acquired until it is broken."),
    LeaseIsBreakingAndCannotBeChanged(
            "The lease ID matched, but the lease is currently in breaking state and cannot be changed."),
    LeaseIsBrokenAndCannotBeRenewed("The lease ID matched, but the lease has been broken explicitly and cannot be renewed."),
    LeaseLost("A lease ID was specified, but the lease for the resource has expired."),
    LeaseNameMismatch("The lease name specified did not match the existing lease name."),
    LeaseNotPresent("There is currently no lease on the resource."),
    LeaseNotPresentWithLeaseOperation("The lease ID is not present with the specified lease operation."),
    MissingRequiredHeader("An HTTP header that's mandatory for this request is not specified."),
    MissingRequiredQueryParameter("A query parameter that's mandatory for this request is not specified."),
    MultipleConditionHeadersNotSupported("Multiple condition headers are not supported."),
    OperationTimedOut("The operation could not be completed within the permitted time."),
    OutOfRangeInput("One of the request inputs is out of range."),
    OutOfRangeQueryParameterValue("One of the query parameters specified in the request URI is outside the permissible range."),
    PathAlreadyExists("The specified path already exists."),
    PathConflict("The specified path, or an element of the path, exists and its resource type is invalid for this operation."),
    PathNotFound("The specified path does not exist."),
    RenameDestinationParentPathNotFound("The parent directory of the destination path does not exist."),
    RequestBodyTooLarge("The request body is too large and exceeds the maximum permissible limit."),
    ResourceNotFound("The specified resource does not exist."),
    ResourceTypeMismatch("The resource type specified in the request does not match the type of the resource."),
    ServerBusy("The server is currently unable to receive requests. Please retry your request."),
    SourceConditionNotMet("The source condition specified using HTTP conditional header(s) is not met."),
    SourcePathIsBeingDeleted("The specified source path is marked to be deleted."),
    SourcePathNotFound("The source path for a rename operation does not exist."),
    UnsupportedHeader("One of the headers specified in the request is not supported."),
    UnsupportedHttpVerb("The resource doesn't support the specified HTTP verb."),
    UnsupportedQueryParameter("One of the query parameters specified in the request URI is not supported."),
    UnsupportedRestVersion("The specified Rest Version is Unsupported.");

    private final String errorMessage;

    /**
     * @param text
     */
    ApiErrors(final String text) {
        this.errorMessage = text;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString() {
        return errorMessage;
    }
}
