package com.itv.bucky.prometheus;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.impl.AbstractMetricsCollector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.CollectorRegistry;

public class PrometheusMetricsCollector extends AbstractMetricsCollector {

	private final Gauge connections;

	private final Gauge channels;

	private final Counter publishedMessages;

	private final Counter failedToPublishMessages;

	private final Counter ackedPublishedMessages;

	private final Counter nackedPublishedMessages;

	private final Counter unroutedPublishedMessages;

	private final Counter consumedMessages;

	private final Counter acknowledgedMessages;

	private final Counter rejectedMessages;

	public PrometheusMetricsCollector(final CollectorRegistry registry) {
		this(registry, "rabbitmq");
	}

	public PrometheusMetricsCollector(final CollectorRegistry registry, final String prefix) {
		this.connections = Gauge.build()
      .name(prefix + "_connections")
      .help("Current connections")
      .create();
		registry.register(this.connections);

		this.channels = Gauge.build()
      .name(prefix + "_channels")
      .help("Current channels")
      .create();
		registry.register(this.channels);

		this.publishedMessages = Counter.build()
      .name(prefix + "_published_messages")
      .help("Count of published messages")
      .create();
		registry.register(this.publishedMessages);

		this.failedToPublishMessages = Counter.build()
      .name(prefix + "_failed_to_publish_messages")
      .help("Count of failed to publish messages")
      .create();
		registry.register(failedToPublishMessages);

		this.ackedPublishedMessages = Counter.build()
      .name(prefix + "_acked_published_messages")
      .help("Count of acknowledged publish messages")
      .create();
		registry.register(ackedPublishedMessages);

		this.nackedPublishedMessages = Counter.build()
      .name(prefix + "_nacked_published_messages")
      .help("Count of not acknowledged publish messages")
      .create();
		registry.register(nackedPublishedMessages);

		this.unroutedPublishedMessages = Counter.build()
      .name(prefix + "_unrouted_published_messages")
      .help("Count of unrouted publish messages")
      .create();
		registry.register(unroutedPublishedMessages);

		this.consumedMessages = Counter.build()
      .name(prefix + "_consumed_messages")
      .help("Count of consumed messages")
      .create();
		registry.register(consumedMessages);

		this.acknowledgedMessages = Counter.build()
      .name(prefix + "_acknowledged_messages")
      .help("Count of acknowledged consumed messages")
      .create();
		registry.register(acknowledgedMessages);

		this.rejectedMessages = Counter.build()
      .name(prefix + "_rejected_messages")
      .help("Count of rejected consumed messages")
      .create();
		registry.register(rejectedMessages);

	}

	@Override
	protected void incrementConnectionCount(Connection connection) {
		connections.inc();
	}

	@Override
	protected void decrementConnectionCount(Connection connection) {
		connections.dec();
	}

	@Override
	protected void incrementChannelCount(Channel channel) {
		channels.inc();
	}

	@Override
	protected void decrementChannelCount(Channel channel) {
		channels.dec();
	}

	@Override
	protected void markPublishedMessage() {
		publishedMessages.inc();
	}

	@Override
	protected void markMessagePublishFailed() {
		failedToPublishMessages.inc();
	}

	@Override
	protected void markConsumedMessage() {
		consumedMessages.inc();
	}

	@Override
	protected void markAcknowledgedMessage() {
		acknowledgedMessages.inc();
	}

	@Override
	protected void markRejectedMessage() {
		rejectedMessages.inc();
	}

	@Override
	protected void markMessagePublishAcknowledged() {
		ackedPublishedMessages.inc();
	}

	@Override
	protected void markMessagePublishNotAcknowledged() {
		nackedPublishedMessages.inc();
	}

	@Override
	protected void markPublishedMessageUnrouted() {
		unroutedPublishedMessages.inc();
	}
}
