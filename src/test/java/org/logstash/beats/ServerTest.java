package org.logstash.beats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.number.IsCloseTo.closeTo;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class ServerTest {

    private int randomPort;
    private EventLoopGroup group;
    private final String host = "0.0.0.0";
    private final int threadCount = 10;

    @Before
    public void setUp() {
        randomPort = tryGetPort();
        group = new NioEventLoopGroup();
    }

    @After
    public void shutdown() {
        group.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testServerShouldTerminateConnectionWhenExceptionHappen() throws InterruptedException, ExecutionException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        AtomicInteger connected = new AtomicInteger(0);

        CountDownLatch latch = new CountDownLatch(concurrentConnections);

        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setClientInactivityTimeout(inactivityTime)
                        .setBeatsHeandlerThreadCount(threadCount)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setChannelClass(NioServerSocketChannel.class);

        final AtomicBoolean otherCause = new AtomicBoolean(false);
        server.setMessageListener(new MessageListener() {
            public void onNewConnection(ChannelHandlerContext ctx) {
                // Make sure connection is closed on exception too.
                if (connected.incrementAndGet() == 1) {
                    throw new RuntimeException("Dummy");
                }
            }

            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                // Make sure connection is closed on exception too.
                throw new RuntimeException("Dummy");
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                // Make sure only intended exception is thrown
                if (!"Dummy".equals(cause.getMessage())) {
                    otherCause.set(true);
                }
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        try {
            ChannelFutureListener cfl = this::simpleSend;
            for (int i = 0; i < concurrentConnections; i++) {
                connectClient().addListener(cfl);
            }
            assertThat(latch.await(10, TimeUnit.SECONDS), is(true));
            assertThat(otherCause.get(), is(false));
        } finally {
            server.stop();
            thread.join();
        }
    }

    @Test(timeout=10000)
    public void testOverSizedBatch() throws InterruptedException, ExecutionException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger lastGoodCount = new AtomicInteger();
        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setMaxPayloadSize(100)
                        .setClientInactivityTimeout(1)
                        .setBeatsHeandlerThreadCount(threadCount)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setChannelClass(NioServerSocketChannel.class);

        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                lastGoodCount.set(message.getData().size());
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                latch.countDown();
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        ChannelFutureListener incrementSender = nf -> this.incrementSend(nf, 1);

        try {
            ChannelFuture cf = connectClient();
            cf.addListener(incrementSender);
            assertThat(latch.await(1, TimeUnit.SECONDS), is(true));
            assertThat(lastGoodCount.get(), is(6));
        } finally {
            server.stop();
            thread.join();
        }
    }

    private void incrementSend(ChannelFuture f, int count) throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            batch.setBatchSize(1);
            ByteBuf contents = V2BatchTest.messageContents(count);
            batch.addMessage(0, contents, contents.readableBytes());
            ChannelFuture cf = f.channel().writeAndFlush(batch);
            if (count < 10 && f.channel().isOpen()) {
                ChannelFutureListener incrementSender = nf -> this.incrementSend(nf, count + 1);
                cf.addListener(incrementSender);
            } else {
                cf.channel().close();
            }
        }
    }

    @Test(timeout=10000)
    public void testServerShouldTerminateConnectionIdleForTooLong() throws InterruptedException, ExecutionException {
        int inactivityTime = 3; // in seconds
        int concurrentConnections = 10;

        CountDownLatch latch = new CountDownLatch(concurrentConnections);
        AtomicBoolean exceptionClose = new AtomicBoolean(false);
        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setShutdownDelay(100, TimeUnit.MILLISECONDS)
                        .setClientInactivityTimeout(inactivityTime)
                        .setBeatsHeandlerThreadCount(threadCount);
        server.setMessageListener(new MessageListener() {
            @Override
            public void onConnectionClose(ChannelHandlerContext ctx) {
                latch.countDown();
            }

            @Override
            public void onException(ChannelHandlerContext ctx, Throwable cause) {
                exceptionClose.set(true);
            }
        });

        Thread thread = new Thread(() -> serverRun(server));
        thread.start();
        server.f.get();

        try {
            long started = System.currentTimeMillis();

            for (int i = 0; i < concurrentConnections; i++) {
                connectClient();
            }
            assertThat(latch.await(10, TimeUnit.SECONDS), is(true));

            long ended = System.currentTimeMillis();

            long diff = ended - started;
            assertThat(diff / 1000.0, is(closeTo(inactivityTime, .5)));
            assertThat(exceptionClose.get(), is(false));
        } finally {
            server.stop();
            thread.join();
        }
    }

    @Test(timeout=10000)
    public void testServerShouldAcceptConcurrentConnection() throws InterruptedException, ExecutionException {
        // Each connection is sending 1 batch.
        int ConcurrentConnections = 5;

        CountDownLatch latch = new CountDownLatch(ConcurrentConnections);
        CountDownLatch startLatch = new CountDownLatch(1);

        Server server = new Server()
                        .setHost(host)
                        .setPort(randomPort)
                        .setClientInactivityTimeout(30)
                        .setBeatsHeandlerThreadCount(threadCount);

        server.setMessageListener(new MessageListener() {
            @Override
            public void onNewMessage(ChannelHandlerContext ctx, Message message) {
                latch.countDown();
            }
        });

        new Thread(() -> serverRun(server)).start();
        server.f.get();

        ChannelFutureListener opCompleted = f -> operationComplete(startLatch, f);
        for (int i = 0; i < ConcurrentConnections; i++) {
            new Thread(() -> connect(startLatch, opCompleted)).start();
        }

        startLatch.countDown();
        latch.await();
        server.stop();
    }

    private void operationComplete(CountDownLatch startLatch, ChannelFuture future) throws InvalidFrameProtocolException, InterruptedException {
        startLatch.await();
        simpleSend(future);
        future.channel().close();
    }

    private void connect(CountDownLatch startLatch, ChannelFutureListener opCompleted) {
        try {
            startLatch.await();
            connectClient().addListener(opCompleted);
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
    }

    public ChannelFuture connectClient() {
        Bootstrap b = new Bootstrap();
        b.group(group)
        .channel(NioSocketChannel.class)
        .handler(new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new BatchEncoder());
            }
        });
        return b.connect("localhost", randomPort);
    }

    private void simpleSend(ChannelFuture f) throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            batch.setBatchSize(1);
            ByteBuf contents = V2BatchTest.messageContents();
            batch.addMessage(1, contents, contents.readableBytes());
            f.channel().writeAndFlush(batch);
        }
    }

    /**
     * Try to find a random available port
     * @return an available listen port
     */
    private static int tryGetPort() {
        try (ServerSocket ss = new ServerSocket(0)){
            ss.setReuseAddress(true);
            return ss.getLocalPort();
        } catch (IOException e) {
            return -1;
        }
    }

    private void serverRun(Server s) {
        try {
            s.listen();
        } catch (InterruptedException e) {
        }
    }

}
