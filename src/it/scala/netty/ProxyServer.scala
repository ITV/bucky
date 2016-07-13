package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.typesafe.scalalogging.StrictLogging
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.channel._
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory


object ProxyServer {

  val executor = Executors.newCachedThreadPool()
  val factory = new NioServerSocketChannelFactory(executor, executor)
  val bootstrap = new ServerBootstrap(factory)
  val handler = ProxyHandler
  val pipeline = bootstrap.getPipeline
  pipeline.addLast("handler", handler)
  bootstrap.bind(new InetSocketAddress(8888))


  object NettyUtil {

    private val addressPortRE = """^(.*):(.*)$""".r

    def parseAddressPortString(
                                addressPortString: String): InetSocketAddress = {
      addressPortString match {
        case addressPortRE(addressString, portString) =>
          new InetSocketAddress(addressString, portString.toInt)

        case _ => throw new IllegalArgumentException(
          "Bad address:port string '" + addressPortString + "'")
      }
    }

    def closeOnFlush(channel: Channel) {
      if (channel.isConnected) {
        channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(
          ChannelFutureListener.CLOSE)
      } else {
        channel.close
      }
    }

  }


  private class RemoteChannelHandler(
                                      val clientChannel: Channel)
    extends SimpleChannelUpstreamHandler with StrictLogging {

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.info("remote channel open " + e.getChannel)
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      logger.warn("remote channel exception caught " + e.getChannel, e.getCause)
      e.getChannel.close
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.info("remote channel closed " + e.getChannel)
      NettyUtil.closeOnFlush(clientChannel)
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      clientChannel.write(e.getMessage)
    }

  }

  private class ClientChannelHandler(
                                      remoteAddress: InetSocketAddress,
                                      clientSocketChannelFactory: ClientSocketChannelFactory)
    extends SimpleChannelUpstreamHandler with StrictLogging {

    @volatile
    private var remoteChannel: Channel = null

    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      val clientChannel = e.getChannel
      logger.info("client channel open " + clientChannel)
      clientChannel.setReadable(false)

      val clientBootstrap = new ClientBootstrap(
        clientSocketChannelFactory)
      clientBootstrap.setOption("connectTimeoutMillis", 1000)
      clientBootstrap.getPipeline.addLast("handler",
        new RemoteChannelHandler(clientChannel))

      val connectFuture = clientBootstrap
        .connect(remoteAddress)
      remoteChannel = connectFuture.getChannel
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
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      logger.info("client channel exception caught " + e.getChannel,
        e.getCause)
      e.getChannel.close
    }

    override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      logger.info("client channel closed " + e.getChannel)
      val remoteChannelCopy = remoteChannel
      if (remoteChannelCopy != null) {
        NettyUtil.closeOnFlush(remoteChannelCopy)
      }
    }

    override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
      val remoteChannelCopy = remoteChannel
      if (remoteChannelCopy != null) {
        remoteChannelCopy.write(e.getMessage)
      }
    }

  }

  private class ProxyPipelineFactory(
                                      val remoteAddress: InetSocketAddress,
                                      val clientSocketChannelFactory: ClientSocketChannelFactory)
    extends ChannelPipelineFactory {

    override def getPipeline: ChannelPipeline =
      Channels.pipeline(new ClientChannelHandler(remoteAddress,
        clientSocketChannelFactory))

  }

  object ProxyHandler extends SimpleChannelUpstreamHandler {

    override def messageReceived(context: ChannelHandlerContext, e: MessageEvent): Unit = {
      e.getChannel.write(e.getMessage)
    }

    override def exceptionCaught(context: ChannelHandlerContext, e: ExceptionEvent): Unit =  {
      e.getChannel.close()
    }

  }


}
