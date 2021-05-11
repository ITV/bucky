![Cats Friendly Badge](https://typelevel.org/cats/img/cats-badge-tiny.png) 

[![Build Status](https://travis-ci.org/ITV/bucky.svg?branch=master)](https://travis-ci.org/ITV/bucky)
[![Join the chat at https://gitter.im/com.itv-bucky/Lobby](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/itv-bucky/Lobby)

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