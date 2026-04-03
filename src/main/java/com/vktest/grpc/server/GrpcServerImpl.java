package com.vktest.grpc.server;

import com.google.protobuf.ByteString;
import com.vktest.grpc.proto.*;
import com.vktest.grpc.tarantool.TarantoolConnection;
import io.grpc.stub.StreamObserver;

public class GrpcServerImpl extends KeyValueServiceGrpc.KeyValueServiceImplBase {
    private final TarantoolConnection tarantool;

    public GrpcServerImpl(TarantoolConnection tarantool) {
        this.tarantool = tarantool;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        String key = request.getKey();
        byte[] value = null;
        
        if (request.hasValue()) {
            value = request.getValue().toByteArray();
        }
        
        tarantool.put(key, value).whenComplete((success, throwable) -> {
            PutResponse response = PutResponse.newBuilder()
                    .setSuccess(success)
                    .setErrorMessage(throwable != null ? throwable.getMessage() : "")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        String key = request.getKey();

        tarantool.get(key).whenComplete((optionalValue, throwable) -> {
            GetResponse.Builder builder = GetResponse.newBuilder();
            
            if (throwable != null) {
                builder.setFound(false).setErrorMessage(throwable.getMessage());
            } else if (optionalValue != null) {
                builder.setFound(true);
                if (optionalValue.isPresent()) {
                    byte[] value = optionalValue.get();
                    if (value != null) {
                        builder.setValue(ByteString.copyFrom(value));
                    }
                }
            } else {
                builder.setFound(false);
            }
            
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        String key = request.getKey();

        tarantool.delete(key).whenComplete((found, throwable) -> {
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setSuccess(throwable == null)
                    .setFound(found)
                    .setErrorMessage(throwable != null ? throwable.getMessage() : "")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void range(RangeRequest request, StreamObserver<RangeResponse> responseObserver) {
        String keySince = request.getKeySince();
        String keyTo = request.getKeyTo();

        tarantool.range(keySince, keyTo, responseObserver);
    }

    @Override
    public void count(CountRequest request, StreamObserver<CountResponse> responseObserver) {
        tarantool.count().whenComplete((count, throwable) -> {
            CountResponse response = CountResponse.newBuilder()
                    .setCount(count)
                    .setErrorMessage(throwable != null ? throwable.getMessage() : "")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }
}