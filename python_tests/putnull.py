import grpc
import service_pb2
import service_pb2_grpc

channel = grpc.insecure_channel('localhost:8080')
stub = service_pb2_grpc.KeyValueServiceStub(channel)

request = service_pb2.PutRequest(key="null_test2222")
stub.Put(request)