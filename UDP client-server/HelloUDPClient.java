package info.kgeorgiy.ja.mozzhevilov.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;

import java.io.IOException;
import java.net.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static info.kgeorgiy.ja.mozzhevilov.hello.HelloUDPUtils.*;

public class HelloUDPClient implements HelloClient {

    @Override
    public void run(final String host, final int port, final String prefix, final int threadsCount, final int requests) {
        final ExecutorService threads = Executors.newFixedThreadPool(threadsCount);
        final List<Future<?>> arr = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++) {
            final int num = i;
            arr.add(threads.submit(() -> sendAndReceive(host, port, prefix, num, requests)));
        }
        try {
            for (final Future<?> future : arr) {
                future.get();
            }
        } catch (final ExecutionException | InterruptedException ignored) {
            // pass
        }
        threads.shutdown();
    }

    private SocketAddress createSocketAddress(final String host, final int port) throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getByName(host), port);
    }

    private byte[] createData(final String prefix, final int threadNumber, final int requestNumber) {
        final String query = prefix + threadNumber + "_" + requestNumber;
        return query.getBytes();
    }

    private void sendAndReceive(final String host, final int port, final String prefix,
                                final int threadNumber, final int requests) {
        try(final DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(HelloUDPUtils.TIMEOUT);
            final SocketAddress socketAddress;
            try {
                socketAddress = createSocketAddress(host, port);
            } catch (final UnknownHostException e) {
                return;
            }

            final int bufferSize = socket.getReceiveBufferSize();
            final byte[] buffer = new byte[bufferSize];

            final DatagramPacket requestPacket = new DatagramPacket(new byte[0], 0, socketAddress);
            final DatagramPacket receivePacket = new DatagramPacket(buffer, bufferSize);

            for (int i = 0; i < requests; i++) {
                requestPacket.setData(createData(prefix, threadNumber, i));
                receivePacket.setData(buffer);

                while (!Thread.interrupted() && !socket.isClosed()) {
                    try {
                        socket.send(requestPacket);
                        socket.receive(receivePacket);
                        if (verify(getDatagramPacketDataAsString(receivePacket), threadNumber, i)) {
                            break;
                        }
                    } catch (final IOException e) {
                    }
                }
            }
        } catch (final SocketException e) {
        }
    }

    public static void main(final String[] args) {
        runClientMain(args, HelloUDPClient::new);
    }
}
