#!/usr/bin/env python3
import grpc
import sys
sys.path.append('src/main/proto')

import service_pb2
import service_pb2_grpc

channel = grpc.insecure_channel('localhost:8080')
stub = service_pb2_grpc.KeyValueServiceStub(channel)

total = 5000000
for i in range(1, total + 1):
    request = service_pb2.PutRequest(
        key=f"key_{i}",
        value=f"value_{i}".encode()
    )
    stub.Put(request)
    
    if i % 10000 == 0:
        print(f"Записано {i} записей")