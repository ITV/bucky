![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

(under construction: [v1 docs](https://github.com/ITV/bucky/tree/v1.4.5), [v2 docs](https://io.itv.com/bucky/))
    
# Migrating to 2.0.0-M28 and above

Version 2.0.0-M28 introduced a change to default all dead letter exchanges to be Fanout by default. Previously these were Direct.
The reason for this change is due to an issue when multiple queues are bound to the same routing key on the same exchange and vice versa.
When a handler dead letters a message it will be lost into the Ether as the broker can't work out where to send it.

To upgrade:
 - The signature of `requeueDeclarations` has changed. Try to use the new default dlx exchange type where possible.
 - If changing the dlx type, delete the `.requeue`, `.dlx` and `.redeliver` exchanges manually before deploying your newly upgraded service.
 If you don't do this, the service will fail to start complaining about mismatching Exchange types.

If you really must continue using a Direct exchange:
 - If using Wiring, use `setDeadLetterExchangeType = ExchangeType.Direct`
 - If using requeueDeclarations, you will need to pass in `dlxType=Direct`.


# Migrating to 2.0.0-M30 and above

Version 2.0.0-M30 introduced a change to the `Wiring` module where, if a requeue policy is being explicitly set, the `retryAfter` value is
passed into the `x-message-ttl` parameter of the requeue queue, which previously defaulted to 5 minutes. 
This means that the `retryAfter` value being declared will be respected.
In this scenario, as the queue policy is being changed, it may need to the requeue queue to be manually deleted so that the application can recreate it. The application may fail to start up otherwise.

# Migrating to 4.0.0-M1 and above

Version 4.0.0-M1 introduced significant architectural changes to support multiple AMQP backends. The main client implementation has been moved into separate backend modules, allowing you to choose between the traditional Java AMQP client or the new fs2-rabbit backend.

## Breaking Changes

### 1. Client Implementation Moved to Backend Modules

The `AmqpClient` is now a trait, and concrete implementations have moved to separate modules:

- **Java AMQP Backend** (traditional): `com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient`
- **fs2-rabbit Backend** (new): `com.itv.bucky.backend.fs2rabbit.Fs2RabbitAmqpClient`

**Migration:**
Replace your import and client creation:

```scala
// Before (v3.1.5)
import com.itv.bucky.AmqpClient

AmqpClient[IO](amqpClientConfig).use { client =>
  // ...
}

// After (v4.0.0-M1) - Using Java AMQP Backend
import com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient

JavaBackendAmqpClient[IO](amqpClientConfig).use { client =>
  // ...
}

// After (v4.0.0-M1) - Using fs2-rabbit Backend
import com.itv.bucky.backend.fs2rabbit.Fs2RabbitAmqpClient

Fs2RabbitAmqpClient[IO](amqpClientConfig).use { client =>
  // ...
}
```

### 2. Dependencies Need Updating

Add the appropriate backend module dependency to your `build.sbt`:

```scala
// For Java AMQP Backend (traditional)
libraryDependencies += "com.itv" %% "bucky-backend-java-amqp" % "4.0.0-M1"

// For fs2-rabbit Backend (new)
libraryDependencies += "com.itv" %% "bucky-backend-fs2-rabbit" % "4.0.0-M1"
```

Note: The core module (`bucky-core`) is automatically included as a dependency of both backend modules.

### 3. Publisher Methods Now Return `F[Publisher[F, T]]`

Previously, publisher methods returned `Publisher[F, T]` directly. Now they return `F[Publisher[F, T]]` (wrapped in effect).

**Migration:**

```scala
// Before (v3.1.5)
val publisher = client.publisherOf[Person](exchangeName, routingKey)
publisher(person)

// After (v4.0.0-M1)
for {
  publisher <- client.publisherOf[Person](exchangeName, routingKey)
  _         <- publisher(person)
} yield ()
```

This applies to all publisher creation methods:
- `client.publisher()`
- `client.publisherOf[T](...)`
- `client.publisherWithHeadersOf[T](...)`

### 4. Wiring Publisher Methods Updated

If you're using the `Wiring` class, the publisher and publisherWithHeaders methods now return `F[Publisher[F, T]]`:

```scala
// Before (v3.1.5)
val publisher = wiring.publisher(client)

// After (v4.0.0-M1)
for {
  publisher <- wiring.publisher(client)
} yield publisher
```

### 5. New AmqpClientConfig Parameters

The `AmqpClientConfig` has new optional parameters (with sensible defaults):

```scala
case class AmqpClientConfig(
  host: String,
  port: Int,
  username: String,
  password: String,
  networkRecoveryInterval: Option[FiniteDuration] = Some(3.seconds),
  networkRecoveryIntervalOnStart: Option[NetworkRecoveryOnStart] = Some(NetworkRecoveryOnStart()),
  publishingTimeout: FiniteDuration = 15.seconds,
  virtualHost: Option[String] = None,
  connectionTimeout: FiniteDuration = 10.seconds,        // NEW
  ssl: Boolean = false,                                   // NEW
  requeueOnNack: Boolean = false,                        // NEW
  requeueOnReject: Boolean = true,                       // NEW
  internalQueueSize: Option[Int] = None                  // NEW
)
```

You don't need to change existing code unless you want to use these new options.

### 6. Optional: Mandatory Publishing Support

Publishers now support an optional `mandatory` flag for publish confirmations:

```scala
// Create a publisher with mandatory=true
client.publisher(mandatory = true)
client.publisherOf[T](mandatory = true)
client.publisherOf[T](exchangeName, routingKey, mandatory = true)
```

**⚠️ GOTCHA: `usingMandatory` Method Order Matters on v4.0.0-M1 only**

If you're using `PublishCommandBuilder` directly and need mandatory publishing, you **must** call `.usingMandatory(true)` **before** completing both the exchange and routing key. Once both are set, the builder returns a type that lacks the `usingMandatory` method.

This is fixed in v4.0.0-M2 and later, but if you're on M1, be mindful of the order:

```scala
// ❌ THIS WILL NOT COMPILE in v4.0.0-M1
val builder = PublishCommandBuilder
  .publishCommandBuilder(marshaller)
  .using(exchangeName)
  .using(routingKey)
  .usingMandatory(true)  // ERROR: Builder[T] doesn't have usingMandatory method!

// ✅ CORRECT - Call usingMandatory BEFORE both exchange and routingKey are set
val builder = PublishCommandBuilder
  .publishCommandBuilder(marshaller)
  .usingMandatory(true)  // Call this early!
  .using(exchangeName)
  .using(routingKey)

// ✅ ALSO CORRECT - Call after exchange but before routingKey
val builder = PublishCommandBuilder
  .publishCommandBuilder(marshaller)
  .using(exchangeName)
  .usingMandatory(true)  // Still works here
  .using(routingKey)

// ✅ ALSO CORRECT - Call after routingKey but before exchange
val builder = PublishCommandBuilder
  .publishCommandBuilder(marshaller)
  .using(routingKey)
  .usingMandatory(true)  // Still works here
  .using(exchangeName)
```

**Note:** This was a change from v3.1.5 where `usingMandatory` could be called at any point in the builder chain. In v4.0.0-M1, the final `Builder` type (after both exchange and routingKey are set) no longer has this method.

## Migration Checklist

1. ✅ Update your dependencies in `build.sbt` to include the appropriate backend module
2. ✅ Replace `AmqpClient[F](config)` with `JavaBackendAmqpClient[F](config)` or `Fs2RabbitAmqpClient[F](config)`
3. ✅ Update imports to use the backend-specific client
4. ✅ Wrap publisher usage in `for-comprehension` or `flatMap` to handle `F[Publisher[F, T]]` return type
5. ✅ If using `Wiring`, update publisher creation to handle `F[Publisher[F, T]]`
6. ✅ If using `PublishCommandBuilder` with `.usingMandatory()`, ensure it's called **before** both exchange and routing key are set if using v4.0.0-M1. This is fixed in M2 onwards.
7. ✅ Test your application thoroughly

## Example Migration

**Before (v3.1.5):**
```scala
import cats.effect.{ExitCode, IO, IOApp}
import com.itv.bucky._
import com.itv.bucky.decl._

object Example extends IOApp {
  val config = AmqpClientConfig("localhost", 5672, "guest", "guest")
  
  override def run(args: List[String]): IO[ExitCode] =
    AmqpClient[IO](config).use { client =>
      for {
        _         <- client.declare(declarations)
        publisher = client.publisherOf[Person](exchange, routingKey)
        _         <- publisher(Person("Alice", 30))
      } yield ExitCode.Success
    }
}
```

**After (v4.0.0-M1):**
```scala
import cats.effect.{ExitCode, IO, IOApp}
import com.itv.bucky._
import com.itv.bucky.backend.javaamqp.JavaBackendAmqpClient  // NEW IMPORT
import com.itv.bucky.decl._

object Example extends IOApp {
  val config = AmqpClientConfig("localhost", 5672, "guest", "guest")
  
  override def run(args: List[String]): IO[ExitCode] =
    JavaBackendAmqpClient[IO](config).use { client =>  // CHANGED
      for {
        _         <- client.declare(declarations)
        publisher <- client.publisherOf[Person](exchange, routingKey)  // CHANGED: <- instead of =
        _         <- publisher(Person("Alice", 30))
      } yield ExitCode.Success
    }
}
```

## Choosing Between Backends

- **JavaBackendAmqpClient**: Use this if you're migrating from v3.x.x and want minimal changes. This is the same underlying implementation as before, just moved to a separate module.

- **Fs2RabbitAmqpClient**: Use this if you want to leverage fs2-rabbit's features and are comfortable with fs2 streams. This is a new backend implementation.

Both backends implement the same `AmqpClient[F]` trait, so switching between them should only require changing the client creation and imports.

# Releasing a new version

1. Merge your change into master
2. perform an `sbt release` setting the new version when prompted
3. Release in sonatype