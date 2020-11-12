package org.logstash.beats;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class Server {

    private static final Logger logger = LogManager.getLogger();

    private int port;
    private String host;
    private int clientInactivityTimeoutSeconds;
    private int maxPayloadSize = BeatsInitializer.DEFAULT_MAX_PAYLOAD_SIZE;
    private int beatsHeandlerThreadCount = 1;
    private Supplier<EventLoopGroup> workGroupSupplier = NioEventLoopGroup::new;
    private EventLoopGroup workGroup;
    private Class<? extends ServerChannel> channelClass = NioServerSocketChannel.class;
    private ChannelFactory<? extends ServerChannel> channelFactory = null;
    private IMessageListener messageListener = new MessageListener();
    private SslContext tlsContext = null;
    private BeatsInitializer beatsInitializer = null;
    private Channel listenChannel = null;
    private Duration shutdownDelay = Duration.ofSeconds(5);
    public final CompletableFuture<Channel> f = new CompletableFuture<>();
    public Server() {
        try {
            host = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Server setHost(String host) {
        this.host = host;
        return this;
    }

    public Server setPort(int port) {
        this.port = port;
        return this;
    }

    public Server setClientInactivityTimeout(int clientInactivityTimeoutSeconds) {
        this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
        return this;
    }

    public Server setMaxPayloadSize(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
        return this;
    }

    public Server setBeatsHeandlerThreadCount(int beatsHeandlerThreadCount) {
        this.beatsHeandlerThreadCount = beatsHeandlerThreadCount;
        return this;
    }

    public Server enableSSL(SslContext tlsContext) {
        this.tlsContext = tlsContext;
        return this;
    }

    public Server setEventLoopGroupClass(Class<? extends EventLoopGroup> workGroupClass) {
        try {
            workGroup = workGroupClass.getDeclaredConstructor().newInstance();
            this.workGroupSupplier = () -> workGroup;
            return this;
        } catch (SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException e) {
            throw new IllegalArgumentException("Class " + workGroupClass.getName() + " can't be used for a workgroup", e);
        }
    }

    public Server setEventLoopGroupSupplier(Supplier<EventLoopGroup> workGroupClass) {
        this.workGroupSupplier = workGroupClass;
        return this;
    }

    public Server setChannelClass(Class<? extends ServerChannel> channelClass) {
        this.channelClass = channelClass;
        return this;
    }

    public Server setChannelFactory(ChannelFactory<? extends ServerChannel> channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    public Server setMessageListener(IMessageListener listener) {
        messageListener = listener;
        return this;
    }
    
    public Server setShutdownDelay(long amount, TimeUnit unit) {
        shutdownDelay = Duration.ofMillis(unit.toMillis(amount));
        return this;
    }

    public Server listen() throws InterruptedException, IllegalArgumentException, IllegalStateException {
        if (workGroup != null) {
            try {
                logger.debug("Shutting down existing worker group before starting");
                workGroup.shutdownGracefully(shutdownDelay.toMillis(), shutdownDelay.toMillis() * 2, TimeUnit.MILLISECONDS).sync();
            } catch (Exception e) {
                logger.error("Could not shut down worker group before starting", e);
                throw new IllegalStateException("Could not shut down worker group before starting", e);
            }
        }
        workGroup = workGroupSupplier.get();
        try {
            logger.info("Starting server on port: {}", port);

            beatsInitializer = new BeatsInitializer(tlsContext,
                                                    messageListener, clientInactivityTimeoutSeconds,
                                                    beatsHeandlerThreadCount, maxPayloadSize);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
            // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
            .childOption(ChannelOption.SO_LINGER, 0)
            .childHandler(beatsInitializer);

            if (channelFactory != null) {
                server.channelFactory(channelFactory);
            } else if (channelClass != null) {
                server.channel(channelClass);
            } else {
                throw new IllegalArgumentException("No usable channel source");
            }

            listenChannel = server.bind(host, port).sync().channel();
            f.complete(listenChannel);
            listenChannel.closeFuture().sync();
        } finally {
            shutdown();
        }

        return this;
    }

    public void stop() {
        logger.debug("Server shutting down");
        listenChannel.close();
        logger.debug("Server stopped");
    }

    private void shutdown() {
        try {
            if (workGroup != null) {
                workGroup.shutdownGracefully(shutdownDelay.toMillis(), shutdownDelay.toMillis() * 2, TimeUnit.MILLISECONDS).sync();
                workGroup = null;
            }
            if (beatsInitializer != null) {
                beatsInitializer.shutdownEventExecutor();
                beatsInitializer = null;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public boolean isSslEnable() {
        return tlsContext != null;
    }

    private class BeatsInitializer extends ChannelInitializer<SocketChannel> {
        private static final String SSL_HANDLER = "ssl-handler";
        private static final String IDLESTATE_HANDLER = "idlestate-handler";
        private static final String CONNECTION_HANDLER = "connection-handler";
        private static final String BEATS_ACKER = "beats-acker";

        private static final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
        private static final int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;
        private static final int DEFAULT_MAX_PAYLOAD_SIZE = -1;

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener localMessageListener;
        private final int localClientInactivityTimeoutSeconds;
        private final SslContext localTlsContext;
        private final int maxPayloadSize;

        BeatsInitializer(SslContext tlsContext, IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread, int maxPayloadSize) {
            // Keeps a local copy of Server settings, so they can't be modified once it starts listening
            this.localTlsContext = tlsContext;
            this.localMessageListener = messageListener;
            this.localClientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            this.maxPayloadSize = maxPayloadSize;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);
        }

        @Override
        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();

            if (localTlsContext != null) {
                SslHandler sslHandler = localTlsContext.newHandler(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER,
                             new IdleStateHandler(localClientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS, localClientInactivityTimeoutSeconds));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(maxPayloadSize), new BeatsHandler(localMessageListener));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception caught in channel initializer", cause);
            try {
                localMessageListener.onChannelInitializeException(ctx, cause);
            } finally {
                super.exceptionCaught(ctx, cause);
            }
        }

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully(shutdownDelay.toMillis(), shutdownDelay.toMillis() * 2, TimeUnit.MILLISECONDS).sync();
                beatsHandlerExecutorGroup.shutdownGracefully(shutdownDelay.toMillis(), shutdownDelay.toMillis() * 2, TimeUnit.MILLISECONDS).sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

}
