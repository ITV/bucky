package netty

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.{ChannelHandlerContext, ExceptionEvent, MessageEvent, SimpleChannelUpstreamHandler}
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

object EchoServer extends App {

  val executor = Executors.newCachedThreadPool()
  val factory = new NioServerSocketChannelFactory(executor, executor)
  val bootstrap = new ServerBootstrap(factory)
  val handler = EchoHandler
  val pipeline = bootstrap.getPipeline
  pipeline.addLast("handler", handler)
  bootstrap.bind(new InetSocketAddress(8888))



  object EchoHandler extends SimpleChannelUpstreamHandler {

    override def messageReceived(context: ChannelHandlerContext, e: MessageEvent): Unit = {
      e.getChannel.write(e.getMessage)
    }

    override def exceptionCaught(context: ChannelHandlerContext, e: ExceptionEvent): Unit =  {
      e.getChannel.close()
    }

  }
}
