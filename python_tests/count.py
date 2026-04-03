#!/usr/bin/env python3
import grpc
import service_pb2
import service_pb2_grpc

def main():
    channel = grpc.insecure_channel('localhost:8080')
    
    stub = service_pb2_grpc.KeyValueServiceStub(channel)
    
    request = service_pb2.CountRequest()
    
    try:
        response = stub.Count(request)
        print(f"Количество записей: {response.count}")
    except Exception as e:
        pass

if __name__ == "__main__":
    main()