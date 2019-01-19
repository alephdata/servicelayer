from servicelayer import env


# general gRPC settings
GRPC_LB_POLICY = env.get('GRPC_LB_POLICY', 'round_robin')
GRPC_CONN_AGE = env.to_int('GRPC_CONN_AGE', 500)  # ms
