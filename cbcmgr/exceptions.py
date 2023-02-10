##
##

import os
import inspect
import logging


class CBException(Exception):

    def __init__(self, message):
        logger = logging.getLogger(self.__class__.__name__)
        frame = inspect.currentframe().f_back
        (filename, line, function, lines, index) = inspect.getframeinfo(frame)
        filename = os.path.basename(filename)
        self.message = f"{message} [{function}]({filename}:{line})"
        logger.debug(self.message)
        super().__init__(self.message)


class HTTPExceptionError(CBException):
    pass


class GeneralError(CBException):
    pass


class NotAuthorized(CBException):
    pass


class ForbiddenError(CBException):
    pass


class ClusterInitError(CBException):
    pass


class ClusterCloseError(CBException):
    pass


class CbUtilEnvironmentError(CBException):
    pass


class NodeUnreachable(CBException):
    pass


class NodeConnectionTimeout(CBException):
    pass


class NodeConnectionError(CBException):
    pass


class NodeConnectionFailed(CBException):
    pass


class DNSLookupTimeout(CBException):
    pass


class NodeApiError(CBException):
    pass


class AdminApiError(CBException):
    pass


class CollectionGetError(CBException):
    pass


class CollectionUpsertError(CBException):
    pass


class CollectionSubdocUpsertError(CBException):
    pass


class CollectionSubdocGetError(CBException):
    pass


class CollectionRemoveError(CBException):
    pass


class CollectionCountError(CBException):
    pass


class CollectionWaitException(CBException):
    pass


class CollectionCountException(CBException):
    pass


class ScopeWaitException(CBException):
    pass


class BucketWaitException(CBException):
    pass


class QueryError(CBException):
    pass


class QueryEmptyException(CBException):
    pass


class QueryArgumentsError(CBException):
    pass


class IndexStatError(CBException):
    pass


class IndexConnectError(CBException):
    pass


class IndexBucketError(CBException):
    pass


class IndexScopeError(CBException):
    pass


class IndexQueryError(CBException):
    pass


class IndexCollectionError(CBException):
    pass


class IndexInternalError(CBException):
    pass


class ClusterConnectException(CBException):
    pass


class BucketCreateException(CBException):
    pass


class BucketDeleteException(CBException):
    pass


class ScopeCreateException(CBException):
    pass


class IsCollectionException(CBException):
    pass


class CollectionCreateException(CBException):
    pass


class NotFoundError(CBException):
    pass


class CollectionNameNotFound(CBException):
    pass


class IndexNotReady(CBException):
    pass


class ClusterHealthCheckError(CBException):
    pass


class ClusterKVServiceError(CBException):
    pass


class ClusterQueryServiceError(CBException):
    pass


class ClusterViewServiceError(CBException):
    pass


class CouchbaseError(CBException):
    pass


class IndexExistsError(CBException):
    pass


class IndexNotFoundError(CBException):
    pass


class TransientError(CBException):
    pass


class TestPauseError(CBException):
    pass


class BucketStatsError(CBException):
    pass


class BucketNotFound(CBException):
    pass


class CollectionNotDefined(CBException):
    pass


class MissingAuthKey(CBException):
    pass


class MissingSecretKey(CBException):
    pass


class MissingClusterName(CBException):
    pass


class HTTPException(CBException):
    pass


class HTTPForbidden(CBException):
    pass


class RequestValidationError(CBException):
    pass


class InternalServerError(CBException):
    pass


class ClusterNotFound(CBException):
    pass


class ConnectException(CBException):
    pass


class HTTPNotImplemented(CBException):
    pass


class PaginationDataNotFound(CBException):
    pass


class SyncGatewayOperationException(CBException):
    pass


class ClusterNotConnected(CBException):
    pass


class BucketNotConnected(CBException):
    pass


class ScopeNotConnected(CBException):
    pass


class CollectionNotConnected(CBException):
    pass
