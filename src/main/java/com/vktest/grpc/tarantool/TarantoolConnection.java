package com.vktest.grpc.tarantool;

import io.grpc.stub.StreamObserver;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.box.TarantoolBoxSpace;
import io.tarantool.client.box.options.SelectOptions;
import io.tarantool.client.factory.TarantoolFactory;
import io.tarantool.pool.InstanceConnectionGroup;
import io.tarantool.mapping.SelectResponse;
import io.tarantool.mapping.Tuple;
import io.tarantool.core.protocol.BoxIterator;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import com.google.protobuf.ByteString;
import com.vktest.grpc.proto.RangeResponse;


public class TarantoolConnection implements AutoCloseable {
    private final TarantoolBoxClient client;
    private final TarantoolBoxSpace space;

    public TarantoolConnection(String host, int port, int spaceId, String spaceName) throws Exception {
        
        InstanceConnectionGroup connectionGroup = InstanceConnectionGroup.builder()
            .withHost(host)
            .withPort(port)
            .withSize(Integer.parseInt(System.getenv().getOrDefault("CONNECTION_GROUP_SIZE", "3")))
            .build();
        
        this.client = TarantoolFactory.box()
            .withGroups(Arrays.asList(connectionGroup))
            .build();
        
        boolean connected = client.ping().join();
        if (!connected) {
            throw new RuntimeException("Failed to connect to Tarantool");
        }
        
        createSpaceIfNotExists(spaceId, spaceName);
        
        this.space = client.space(spaceName);
    }

    private void createSpaceIfNotExists(int spaceId, String spaceName) {
        try {
            client.eval(String.format("""
                box.schema.space.create('%s', {
                    if_not_exists = true,
                    id = %d,
                    format = {
                        {name = 'key', type = 'string'},
                        {name = 'value', type = 'varbinary', is_nullable = true}
                    }
                }):create_index('primary', {
                    type = 'TREE',
                    parts = {'key'},
                    if_not_exists = true
                })
                """, spaceName, spaceId)).join();
        } catch (Exception e) {

        }
    }

public CompletableFuture<Boolean> put(String key, byte[] value) {
    List<Object> tuple = Arrays.asList(key, value);
    return space.replace(tuple)
            .thenApply(result -> true)
            .exceptionally(ex -> false);
}


    public CompletableFuture<Optional<byte[]>> get(String key) {
        List<String> keys = Arrays.asList(key);
        
        return space.select(keys)
                .thenApply(response -> {
                    List<Tuple<List<?>>> tuples = response.get();
                    
                    if (tuples == null || tuples.isEmpty()) {
                        return Optional.<byte[]>empty();
                    }
                    
                    Tuple<List<?>> tuple = tuples.get(0);
                    List<?> values = tuple.get();
                    
                    if (values.size() > 1 && values.get(1) instanceof byte[]) {
                        return Optional.of((byte[]) values.get(1));
                    }
                    return Optional.<byte[]>empty();
                })
                .exceptionally(ex -> {
                    return Optional.<byte[]>empty();
                });
    }

    public CompletableFuture<Boolean> delete(String key) {
        List<String> keys = Arrays.asList(key);
        return space.delete(keys)
                .thenApply(result -> {
                    if (result != null) {
                        List<?> deletedData = result.get();
                        return deletedData != null && !deletedData.isEmpty();
                    }
                    return false;
                })
                .exceptionally(ex -> {
                    return false;
                });
    }

public void range(String keySince, String keyTo, StreamObserver<RangeResponse> responseObserver) {
    Object afterPosition = null;
    int pageSize = 10;
    boolean hasMore = true;
    
    while (hasMore) {
        SelectOptions options = SelectOptions.builder()
                .withIterator(BoxIterator.GE)
                .withLimit(pageSize)
                .fetchPosition()
                .after(afterPosition)
                .build();
        
        List<Object> key = List.of(keySince);
        
        try {
            SelectResponse<List<Tuple<List<?>>>> response = space.select(key, options).join();
            List<Tuple<List<?>>> page = response.get();
            
            if (page.isEmpty()) {
                break;
            }
            
            boolean shouldStop = false;
            for (Tuple<List<?>> tuple : page) {
                List<?> values = tuple.get();
                if (values.size() >= 2) {
                    String currentKey = (String) values.get(0);
                    if (currentKey.compareTo(keyTo) > 0) {
                        shouldStop = true;
                        break;
                    }
                    RangeResponse reply = RangeResponse.newBuilder()
                            .setKey(currentKey)
                            .setValue(ByteString.copyFrom((byte[]) values.get(1)))
                            .build();
                    responseObserver.onNext(reply);
                }
            }
            
            afterPosition = response.getPosition();
            
            if (shouldStop || afterPosition == null || page.size() < pageSize) {
                hasMore = false;
            }
        } catch (Exception e) {
            responseObserver.onError(e);
            return;
        }
    }
    
    responseObserver.onCompleted();
}

    public CompletableFuture<Long> count() {
        return client.eval("return box.space." + System.getenv().getOrDefault("SPACE_NAME", "KV") + ":count()")
                .thenApply(response -> {
                    List<?> result = response.get();
                    if (result != null && !result.isEmpty()) {
                        Number number = (Number) result.get(0);
                        return number.longValue();
                    }
                    return 0L;
                })
                .exceptionally(ex -> {
                    return 0L;
                });
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.close();
        }
    }
}
