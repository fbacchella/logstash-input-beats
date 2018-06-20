package org.logstash.beats;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

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
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;

public class Server {

    private static final Logger logger = LogManager.getLogger();

    private final int port;
    private final String host;
    private final int beatsHeandlerThreadCount;
    private EventLoopGroup workGroup;
    private Class<? extends ServerChannel> channelClass;
    private ChannelFactory<? extends ServerChannel> channelFactory;
    private IMessageListener messageListener = new MessageListener();
    private SslContext tlsContext;
    private BeatsInitializer beatsInitializer;

    private final int clientInactivityTimeoutSeconds;

    public Server(String host, int p, int timeout, int threadCount) {
        this.host = host;
        port = p;
        clientInactivityTimeoutSeconds = timeout;
        beatsHeandlerThreadCount = threadCount;
    }

    public Server enableSSL(SslContext tlsContext) {
        this.tlsContext = tlsContext;
        return this;
    }

    public Server setEventLoopGroup(EventLoopGroup workGroup) {
        this.workGroup = workGroup;
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

    public Server listen() throws InterruptedException {
        try {
            logger.info("Starting server on port: {}", port);

            beatsInitializer = new BeatsInitializer(isSslEnable(),
                                                    messageListener, clientInactivityTimeoutSeconds,
                                                    beatsHeandlerThreadCount);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
            // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
            .childOption(ChannelOption.SO_LINGER, 0)
            .childHandler(beatsInitializer);

            if (channelClass != null) {
                server.channel(channelClass);
            } else if (channelFactory != null) {
                server.channelFactory(channelFactory);
            }

            Channel channel = server.bind(host, port).sync().channel();
            channel.closeFuture().sync();
        } finally {
            shutdown();
        }

        return this;
    }

    public void stop() {
        logger.debug("Server shutting down");
        shutdown();
        logger.debug("Server stopped");
    }

    private void shutdown() {
        try {
            if (workGroup != null) {
                workGroup.shutdownGracefully().sync();
            }
            if (beatsInitializer != null) {
                beatsInitializer.shutdownEventExecutor();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    public Server setMessageListener(IMessageListener listener) {
        messageListener = listener;
        return this;
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

        private final EventExecutorGroup idleExecutorGroup;
        private final EventExecutorGroup beatsHandlerExecutorGroup;
        private final IMessageListener message;
        private int clientInactivityTimeoutSeconds;

        private boolean enableSSL = false;

        BeatsInitializer(Boolean secure, IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread) {
            enableSSL = secure;
            this.message = messageListener;
            this.clientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);

        }

        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();

            if (enableSSL) {
                SslHandler sslHandler = tlsContext.newHandler(socket.alloc());
                pipeline.addLast(SSL_HANDLER, sslHandler);
            }
            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER,
                             new IdleStateHandler(clientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS, clientInactivityTimeoutSeconds));
            pipeline.addLast(BEATS_ACKER, new AckEncoder());
            pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(this.message));
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.warn("Exception caught in channel initializer", cause);
            try {
                message.onChannelInitializeException(ctx, cause);
            } finally {
                super.exceptionCaught(ctx, cause);
            }
        }

        public void shutdownEventExecutor() {
            try {
                idleExecutorGroup.shutdownGracefully().sync();
                beatsHandlerExecutorGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

}
