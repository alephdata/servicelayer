import logging
from banal import ensure_list
from grpc import RpcError, StatusCode, insecure_channel

from servicelayer import settings
from servicelayer.rpc.ocr_pb2 import Image
# from servicelayer.rpc.common_pb2 import Text
from servicelayer.rpc.ocr_pb2_grpc import RecognizeTextStub
# from servicelayer.rpc.entityextract_pb2 import ExtractedEntity  # noqa
# from servicelayer.rpc.entityextract_pb2_grpc import EntityExtractStub
from servicelayer.util import backoff, service_retries

log = logging.getLogger(__name__)
TEMP_ERRORS = (StatusCode.UNAVAILABLE, StatusCode.RESOURCE_EXHAUSTED)


class RpcMixin(object):
    """Helper mixing to manage the connectivity with a gRPC endpoint."""
    SERVICE = None

    def has_channel(self):
        return self.SERVICE is not None

    @property
    def channel(self):
        """Lazily connect to the RPC service."""
        if not self.has_channel():
            return
        if not hasattr(self, '_channel') or self._channel is None:
            options = (
                # ('grpc.keepalive_time_ms', settings.GRPC_CONN_AGE),
                ('grpc.keepalive_timeout_ms', settings.GRPC_CONN_AGE),
                ('grpc.max_connection_age_ms', settings.GRPC_CONN_AGE),
                ('grpc.max_connection_idle_ms', settings.GRPC_CONN_AGE),
                ('grpc.lb_policy_name', settings.GRPC_LB_POLICY)
            )
            self._channel = insecure_channel(self.SERVICE, options)
        return self._channel

    def reset_channel(self):
        self._channel.close()
        self._channel = None


class TextRecognizerService(RpcMixin):
    SERVICE = settings.OCR_SERVICE

    def Recognize(self, data, languages=None):
        if not self.has_channel():
            log.warning("gRPC: OCR not configured.")
            return

        for attempt in service_retries():
            try:
                service = RecognizeTextStub(self.channel)
                languages = ensure_list(languages)
                image = Image(data=data, languages=languages)
                return service.Recognize(image)
            except RpcError as e:
                log.warning("gRPC [%s]: %s", e.code(), e.details())
                if e.code() not in TEMP_ERRORS:
                    return
                self.reset_channel()
                backoff(failures=attempt)


# class EntityExtractService(RpcMixin):
#     SERVICE = settings.NER_SERVICE

#     def Extract(self, text, languages):
#         if not self.has_channel():
#             log.warning("gRPC: entity extraction not configured.")
#             return

#         for attempt in service_retries():
#             try:
#                 service = EntityExtractStub(self.channel)
#                 req = Text(text=text, languages=languages)
#                 yield from service.Extract(req)
#                 return
#             except RpcError as e:
#                 log.warning("gRPC [%s]: %s", e.code(), e.details())
#                 if e.code() not in TEMP_ERRORS:
#                     return
#                 self.reset_channel()
#                 backoff(failures=attempt)
