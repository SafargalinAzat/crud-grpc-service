package com.vktest.grpc.server;

import com.vktest.grpc.tarantool.TarantoolConnection;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;

public class GrpcServer {
    private final Server server;

    public GrpcServer(int port, TarantoolConnection tarantool) {
        this.server = NettyServerBuilder.forAddress(new InetSocketAddress("0.0.0.0", port))
                .addService(new GrpcServerImpl(tarantool))
                .build();
    }

    public void start() throws IOException {
        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            GrpcServer.this.stop();
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws Exception {

        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_SERVER_PORT", "8080"));

        String host = System.getenv().getOrDefault("TARANTOOL_HOST", "localhost");
        int tarantoolPort = Integer.parseInt(System.getenv().getOrDefault("TARANTOOL_PORT", "3301"));
        int spaceId = Integer.parseInt(System.getenv().getOrDefault("SPACE_ID", "512"));


        String spaceName = System.getenv().getOrDefault("SPACE_NAME", "KV");

        TarantoolConnection tarantool = new TarantoolConnection(host, tarantoolPort, spaceId, spaceName);
        GrpcServer server = new GrpcServer(port, tarantool);
        server.start();
        server.blockUntilShutdown();
    }
}
