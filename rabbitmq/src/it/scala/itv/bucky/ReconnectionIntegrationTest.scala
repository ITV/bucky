package itv.bucky

import java.net.InetSocketAddress
import java.util.concurrent.Executors

import com.typesafe.scalalogging.StrictLogging
import itv.bucky.PublishCommandBuilder._
import itv.bucky.SameThreadExecutionContext.implicitly
import itv.bucky.decl.{DeclarationLifecycle, Queue}
import itv.contentdelivery.lifecycle.{Lifecycle, VanillaLifecycle}
import org.jboss.netty.bootstrap.{ClientBootstrap, ServerBootstrap}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel._
import org.jboss.netty.channel.group.{ChannelGroup, DefaultChannelGroup}
import org.jboss.netty.channel.socket.ClientSocketChannelFactory
import org.jboss.netty.channel.socket.nio.{NioClientSocketChannelFactory, NioServerSocketChannelFactory}
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._
import scala.util.Random

case class HostPort(hostname: String, port: Int) {
  def toJava: InetSocketAddress = new InetSocketAddress(hostname, port)
}

trait Proxy {
  def setAcceptNewConnections(value: Boolean)
  def closeAllConnections: Unit
  def shutdown: Unit
}

case class ProxyLifecycle(local: HostPort, remote: HostPort) extends VanillaLifecycle[Proxy] with StrictLogging {

  override def start: Proxy = {
    def closeOnFlush(channel: Channel) =
      if (channel.isConnected) {
        channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE)
      } else {
        channel.close
      }

    val allChannels: ChannelGroup = new DefaultChannelGroup()
    var acceptNewConnections = true

    class RemoteChannelHandler(val clientChannel: Channel) extends SimpleChannelUpstreamHandler {
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
        closeOnFlush(e.getChannel)
      }

      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit =
        clientChannel.write(e.getMessage)
    }

    class ClientChannelHandler(remoteAddress: InetSocketAddress,
                               clientSocketChannelFactory: ClientSocketChannelFactory) extends SimpleChannelUpstreamHandler {

      @volatile
      private var remoteChannel: Option[Channel] = None

      override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent): Unit = {
        val clientChannel = e.getChannel

        if (!acceptNewConnections) {
          logger.info("Not accepting new connection " + clientChannel)
          clientChannel.close()
          return ()
        }

        logger.info("Client channel opened " + clientChannel)
        clientChannel.setReadable(false)

        val clientBootstrap = new ClientBootstrap(clientSocketChannelFactory)

        clientBootstrap.setOption("connectTimeoutMillis", 1000)
        clientBootstrap.getPipeline.addLast("handler",
          new RemoteChannelHandler(clientChannel))

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
        remoteChannel.foreach(closeOnFlush)
      }

      override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent): Unit = {
        remoteChannel.foreach(_.write(e.getMessage))
      }
    }

    class ProxyPipelineFactory(remoteAddress: InetSocketAddress,
                               clientSocketChannelFactory: ClientSocketChannelFactory) extends ChannelPipelineFactory {

      override def getPipeline: ChannelPipeline =
        Channels.pipeline(new ClientChannelHandler(remoteAddress,
          clientSocketChannelFactory))

    }

    val executor = Executors.newCachedThreadPool

    val serverBootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(executor, executor))

    val clientSocketChannelFactory = new NioClientSocketChannelFactory(executor, executor)

    serverBootstrap.setPipelineFactory(new ProxyPipelineFactory(remote.toJava, clientSocketChannelFactory))

    serverBootstrap.setOption("reuseAddress", true)

    serverBootstrap.bind(local.toJava)
    logger.info("Listening on " + local)

    new Proxy {
      override def shutdown: Unit = clientSocketChannelFactory.shutdown()

      override def closeAllConnections: Unit = allChannels.close()

      override def setAcceptNewConnections(value: Boolean): Unit =
        acceptNewConnections = value
    }
  }

  override def shutdown(instance: Proxy): Unit = instance.shutdown
}

class ReconnectionIntegrationTest extends FunSuite with ScalaFutures {

  import TestUtils._

  test("can publish and consume a messages through the tcp proxy") {
    val queueName = QueueName("proxy" + Random.nextInt())
    val (amqpClientConfig: AmqpClientConfig, _, _) = IntegrationUtils.configAndHttp

    val marshaller: PayloadMarshaller[Unit] = PayloadMarshaller.lift(_ => Payload.from("hello"))

    val pcb = publishCommandBuilder[Unit](marshaller) using ExchangeName("") using RoutingKey(queueName.value)

    val handler = new StubConsumeHandler[Unit]

    val unmarshaller = Unmarshaller.liftResult[Payload, Unit] {
      _.unmarshal[String] match {
        case UnmarshalResult.Success(s) if s == "hello" => UnmarshalResult.Success(())
        case UnmarshalResult.Success(other) => UnmarshalResult.Failure(other + " was not hello")
        case failure: UnmarshalResult.Failure => failure
      }
    }

    val testLifecycle = for {
      proxy <- ProxyLifecycle(local = HostPort("localhost", 9999), remote = HostPort(amqpClientConfig.host, amqpClientConfig.port))
      client <- amqpClientConfig.copy(host = "localhost", port = 9999, networkRecoveryInterval = Some(1.second))
      _ <- DeclarationLifecycle(List(Queue(queueName).notDurable.expires(1.minute)), client)
      publisher <- client.publisherOf(pcb)
      _ <- client.consumer(queueName, AmqpClient.handlerOf(handler, unmarshaller))
    }
      yield (proxy, publisher)

    Lifecycle.using(testLifecycle) { case (proxy, publisher) =>
      handler.receivedMessages shouldBe 'empty

      publisher.apply(()).asTry.futureValue shouldBe 'success

      eventually {
        handler.receivedMessages should have size 1
      }(Eventually.PatienceConfig(5.seconds, 1.second))

      publisher.apply(()).asTry.futureValue shouldBe 'success

      eventually {
        handler.receivedMessages should have size 2
      }(Eventually.PatienceConfig(5.seconds, 1.second))

      proxy.setAcceptNewConnections(false)
      proxy.closeAllConnections

      publisher.apply(()).asTry.futureValue shouldBe 'failure

      proxy.setAcceptNewConnections(true)

      withClue("should be able to publish after broker allows connections again") {
        eventually {
          publisher.apply(()).asTry.futureValue shouldBe 'success
        }(Eventually.PatienceConfig(5.seconds, 1.second))
      }

      eventually {
        handler.receivedMessages should have size 3
      }(Eventually.PatienceConfig(5.seconds, 1.second))
    }
  }


}
