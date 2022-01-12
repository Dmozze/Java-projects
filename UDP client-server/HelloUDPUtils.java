package info.kgeorgiy.ja.mozzhevilov.hello;

import info.kgeorgiy.java.advanced.hello.HelloClient;
import info.kgeorgiy.java.advanced.hello.HelloServer;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class HelloUDPUtils {

    private final static int AWAIT_TERM_SEC = 60;

    public static int TIMEOUT = 300;

    public static void log(String message) {
        System.out.println(message);
    }

    public static void log(String message, Exception e) {
        System.err.println(message);
        //e.printStackTrace();
    }

    public static String getDatagramPacketDataAsString(DatagramPacket datagramPacket) {
        return new String(datagramPacket.getData(), datagramPacket.getOffset(), datagramPacket.getLength(), StandardCharsets.UTF_8);
    }

    public static String getBufferDataAsString(final ByteBuffer buffer) {
        buffer.flip();
        return StandardCharsets.UTF_8.decode(buffer).toString();
    }

    public static boolean areNullArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                System.out.println(i + " arg is null");
                return true;
            }
        }
        return false;
    }

    public static int checkNumber(final String s, final int begin, final String number) {
        int pos = begin;
        while(pos < s.length() && !Character.isDigit(s.charAt(pos))) {
            pos++;
        }
        if (pos == s.length()) {
            return -2;
        }
        int l = pos;
        while(pos < s.length() && Character.isDigit(s.charAt(pos))) {
            pos++;
        }
        if (number.equals(s.substring(l, pos))) {
            return pos;
        } else {
            return -1;
        }
    }

    public static boolean verify(final String data, final int threadNumber, final int requestNumber) {
        int p = checkNumber(data, 0, Integer.toString(threadNumber));
        if (p < 0) {
            return false;
        }
        p = checkNumber(data, p, Integer.toString(requestNumber));
        if (p < 0) {
            return false;
        }
        p = checkNumber(data, p, null);
        return (p == -2);
    }

    public static void closeAndAwaitTerm(final ExecutorService threads) {
        threads.shutdownNow();
        try {
            threads.awaitTermination(AWAIT_TERM_SEC, TimeUnit.SECONDS);
        } catch (final InterruptedException ignored) {
            // pass
        }
    }

    public static boolean closeSelectorAfterException(final Selector selector, final IOException e) {
        try {
            if (selector == null || !selector.isOpen()) {
                log("Troubles with opening selector", e);
                return false;
            } else {
                selector.close();
            }
        } catch (final IOException ec) {
            log("Troubles with closing", ec);
        }
        return true;
    }

    public static boolean closeChannelAfterException(final DatagramChannel channel, final IOException e) {
        try {
            if (channel == null || !channel.isOpen()) {
                log("Troubles with opening channel", e);
                return false;
            } else {
                channel.close();
            }
        } catch (final IOException ec) {
            log("Troubles with closing", ec);
        }
        return true;
    }

    public static <T> T syncPoll(final Queue<T> queue, final Runnable keyTask) {
        synchronized (queue) {
            if (queue.isEmpty()) {
                keyTask.run();
                return null;
            } else {
                return queue.poll();
            }
        }
    }

    public static <T> void syncAdd(final Queue<T> queue, final T elem, final Runnable keyTask, final Selector selector) {
        synchronized (queue) {
            queue.add(elem);
            keyTask.run();
            selector.wakeup();
        }
    }

    private static boolean areBadArgs(final String[] args, final int cnt) {
        if (args == null) {
            System.out.println("Bad args");
            return true;
        }
        if (args.length != cnt) {
            System.out.println("Bad args count");
            return true;
        }
        if (areNullArgs(args)) {
            return true;
        }
        return false;
    }

    public static void runServerMain(final String[] args, Supplier<HelloServer> supplier) {
        if (areBadArgs(args, 2)) {
            return;
        }
        try (final HelloServer server = supplier.get()) {
            server.start(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
            System.in.read(); // waiting before server's close
        } catch (final NumberFormatException e) {
            System.out.println("Bad numbers in arguments: " + e.getMessage());
        } catch (final IOException e) {
            e.printStackTrace();
        }
    }

    public static void runClientMain(final String[] args, Supplier<HelloClient> supplier) {
        if (areBadArgs(args, 5)) {
            return;
        }
        try {
            supplier.get().run(args[0], Integer.parseInt(args[1]), args[2], Integer.parseInt(args[3]), Integer.parseInt(args[4]));
        } catch (final NumberFormatException e) {
            System.out.println("Bad numbers in arguments: " + e.getMessage());
        }
    }
}