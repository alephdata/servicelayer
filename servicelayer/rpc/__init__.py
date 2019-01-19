import grpc

from servicelayer import settings


class RpcMixin(object):
    """Helper mixing to manage the connectivity with a gRPC endpoint."""
    SERVICE = None
    Error = grpc.RpcError
    Status = grpc.StatusCode
    TEMPORARY_ERRORS = (grpc.StatusCode.UNAVAILABLE,
                        grpc.StatusCode.RESOURCE_EXHAUSTED)

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
            self._channel = grpc.insecure_channel(self.SERVICE, options)
        return self._channel

    def reset_channel(self):
        self._channel.close()
        self._channel = None
