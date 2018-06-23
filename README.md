# Netty Beats codec

[![Travis Build Status](https://travis-ci.org/fbacchella/netty-beats.svg)](https://travis-ci.org/fbacchella/netty-beats)

This is a [netty](https://netty.io)'s handler for Elastic's (beats)[https://www.elastic.co/fr/products/beats]. It can only received message, not send them.
It's a fork from the [logstash's plugin](https://github.com/logstash-plugins/logstash-input-beats) that kept only the netty parts.

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

It provid a simple org.logstash.beats.Server` that can handle creation of a listener for you. You can use it with:

```
        IMessageListener Listener = ...;
        
        Server server = new Server()
                        .setHost(host)
                        .setPort(port)
                        .setMessageListener(listener)
                        .setClientInactivityTimeout(inactivityTime)
                        .setBeatsHeandlerThreadCount(threadCount)
                        .setEventLoopGroupClass(NioEventLoopGroup.class)
                        .setChannelClass(NioServerSocketChannel.class);
       Runnable serverTask = new Runnable() {
            @Override
            public void run() {
                try {
                    server.listen();
                } catch (InterruptedException e) {
                }
            }
        };

        Thread thread = new Thread(serverTask);
        thread.start();
        
```

But it can also created and managed manually:

```

    ServerBootstrap server = new ServerBootstrap();
    server.group(workGroup)
           // Since the protocol doesn't support yet a remote close from the server and we don't want to have 'unclosed' socket lying around we have to use `SO_LINGER` to force the close of the socket.
          .childOption(ChannelOption.SO_LINGER, 0)
          .channel(channelClass);
          
          .childHandler(new ChannelInitializer<SocketChannel>() {
          ....
        @Override
        public void initChannel(SocketChannel socket) throws IOException, NoSuchAlgorithmException, CertificateException {
            ChannelPipeline pipeline = socket.pipeline();
            if (enableSSL) {
                SslHandler sslHandler = tlsContext.newHandler(socket.alloc());
                pipeline.addLast(sslHandler);
            }
            pipeline.addLast(idleExecutorGroup, 
                             new IdleStateHandler(clientInactivityTimeoutSeconds, idleStateWriteSeconds, clientInactivityTimeoutSeconds));
            pipeline.addLast(new AckEncoder());
            pipeline.addLast(new ConnectionHandler());
            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(message));
        }
          });

    Channel channel = server.bind(host, port).sync().channel();
    channel.closeFuture().sync();
````

