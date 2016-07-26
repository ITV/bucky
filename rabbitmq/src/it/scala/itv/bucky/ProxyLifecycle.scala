package itv.bucky

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.typesafe.scalalogging.StrictLogging
import itv.contentdelivery.lifecycle.VanillaLifecycle
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.{NioClientSocketChannelFactory, NioServerSocketChannelFactory}
import org.jboss.netty.channel.{Channels, _}

/***
Inspired by aaronriekenberg/Scala-Netty-Proxy
***/

case class HostPort(hostname: String, port: Int) {
  def toJava: InetSocketAddress = new InetSocketAddress(hostname, port)
}

trait Proxy {
  def stopAcceptingNewConnections()
  def startAcceptingNewConnections()
  def closeAllOpenConnections(): Unit
  def shutdown(): Unit
}

private trait AcceptNewConnection {
  def shouldAcceptNewConnection: Boolean
}

object Netty {
  def closeOnFlush(channel: Channel) =
    if (channel.isConnected) {
      channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE)
    } else {
      channel.close
    }
}

private class RemoteChannelHandler(val clientChannel: Channel, allChannels: ChannelGroup) extends SimpleChannelUpstreamHandler with StrictLogging {
  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    logger.info("remote channel open " + e.getChannel)
    allChannels.add(e.getChannel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    logger.warn("remote channel exception caught on " + e.getChannel, e.getCause)
    e.getChannel.close()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    logger.info("remote channel closed " + e.getChannel)
    Netty.closeOnFlush(e.getChannel)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
    clientChannel.write(e.getMessage)
}

private class DoNotAcceptConnections extends SimpleChannelUpstreamHandler with StrictLogging {
  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    logger.info("Proxy server is currently not accepting connections " + e.getChannel)
    e.getChannel.close()
  }
}

private class ClientChannelHandler(remoteAddress: InetSocketAddress,
                           allChannels: ChannelGroup,
                           acceptNewConnection: AcceptNewConnection,
                           clientSocketChannelFactory: ClientSocketChannelFactory) extends SimpleChannelUpstreamHandler with StrictLogging {

  @volatile
  private var remoteChannel: Option[Channel] = None

  override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    val clientChannel = e.getChannel

    if (!acceptNewConnection.shouldAcceptNewConnection) {
      logger.info("Not accepting new connection " + clientChannel)
      clientChannel.close().await()
      return ()
    }

    logger.info("Client channel opened " + clientChannel)
    clientChannel.setReadable(false)

    val clientBootstrap = new ClientBootstrap(clientSocketChannelFactory)

    clientBootstrap.setOption("connectTimeoutMillis", 1000)
    clientBootstrap.getPipeline.addLast("handler",
      new RemoteChannelHandler(clientChannel, allChannels))

    val connectFuture = clientBootstrap
      .connect(remoteAddress)
    remoteChannel = Some(connectFuture.getChannel)
    connectFuture.addListener(new ChannelFutureListener {
      override def operationComplete(future: ChannelFuture) {
        if (future.isSuccess) {
          logger.info("remote channel connect success "
            + remoteChannel)
          clientChannel.setReadable(true)
        } else {
          logger.info("remote channel connect failure "
            + remoteChannel)
          clientChannel.close
        }
      }
    })

    allChannels.add(e.getChannel)
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent): Unit = {
    logger.info("client channel exception caught " + e.getChannel, e.getCause)
    e.getChannel.close
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
    logger.info("client channel closed: " + e.getChannel)
    remoteChannel.foreach(Netty.closeOnFlush)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
    remoteChannel.foreach(_.write(e.getMessage))
  }
}

case class ProxyLifecycle(local: HostPort, remote: HostPort) extends VanillaLifecycle[Proxy] with StrictLogging {

  override def start: Proxy = {

    val allChannels: ChannelGroup = new DefaultChannelGroup()

    val executor = Executors.newCachedThreadPool

    val serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor))

    val clientSocketChannelFactory = new NioClientSocketChannelFactory(executor, executor)


    object acceptNewConnection extends AcceptNewConnection {
      var shouldAcceptNewConnection: Boolean = true
    }

    object DefaultProxyPipelineFactory extends ChannelPipelineFactory {
      override def getPipeline: ChannelPipeline =
        Channels.pipeline(new ClientChannelHandler(remote.toJava, allChannels, acceptNewConnection,
          clientSocketChannelFactory))
    }

    serverBootstrap.setPipelineFactory(DefaultProxyPipelineFactory)

    serverBootstrap.setOption("reuseAddress", true)

    serverBootstrap.bind(local.toJava)
    logger.info("Listening on " + local)


    new Proxy {
      override def shutdown(): Unit = {
        logger.info("Shutting down proxy server")
        clientSocketChannelFactory.shutdown()
      }

      override def closeAllOpenConnections: Unit = {
        logger.info("Closing all open proxy connections")
        allChannels.close().await()
      }

      override def stopAcceptingNewConnections(): Unit = {
        logger.info("Will no longer accept new connections")
        acceptNewConnection.shouldAcceptNewConnection = false
      }

      override def startAcceptingNewConnections(): Unit = {
        logger.info("Will accept new connections")
        acceptNewConnection.shouldAcceptNewConnection = true
      }
    }
  }

  override def shutdown(instance: Proxy): Unit = instance.shutdown()
}