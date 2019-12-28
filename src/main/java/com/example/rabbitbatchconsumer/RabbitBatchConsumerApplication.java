package com.example.rabbitbatchconsumer;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.*;

import java.time.Duration;

@Slf4j
@EnableScheduling
@SpringBootApplication
public class RabbitBatchConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(RabbitBatchConsumerApplication.class, args);
	}

	public static final String QUEUE_NAME = "batch_queue";

	@Bean
	Mono<Connection> connectionMono(RabbitProperties rabbitProperties) {
		ConnectionFactory connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(rabbitProperties.getHost());
		connectionFactory.setPort(rabbitProperties.getPort());
//		connectionFactory.setVirtualHost(rabbitProperties.getVirtualHost());
		connectionFactory.setUsername(rabbitProperties.getUsername());
		connectionFactory.setPassword(rabbitProperties.getPassword());
		return Mono.fromCallable(() -> connectionFactory.newConnection("reactor-rabbit")).cache();
	}

	@Bean
	public Sender sender(Mono<Connection> connectionMono) {
		return RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
	}

	@Bean
	public Receiver receiver(Mono<Connection> connectionMono) {
		return RabbitFlux.createReceiver(new ReceiverOptions().connectionMono(connectionMono));
	}

	@Bean
	public Disposable source(Sender sender) {
		Flux<OutboundMessage> source = Flux.interval(Duration.ofSeconds(1))
				.map(num -> Long.toString(num))
				.map(str -> new OutboundMessage("", QUEUE_NAME, str.getBytes()));

		return sender.declareQueue(QueueSpecification.queue(QUEUE_NAME))
				.thenMany(sender.sendWithPublishConfirms(source))
				.doOnEach(msg -> System.out.println("Sending..."))
				.subscribe();
	}

	@Bean
	public Disposable sink(Sender sender, Receiver receiver) {
		return receiver.consumeManualAck(QUEUE_NAME)
				.delaySubscription(sender.declareQueue(QueueSpecification.queue(QUEUE_NAME)))
				.bufferTimeout(10, Duration.ofSeconds(5))
				.subscribe(list -> {
					log.info("Batch START");
					list.forEach(delivery -> {
						try {
							log.info("Message: {}", new String(delivery.getBody()));
							delivery.ack();
						} catch (Exception ex) {
							delivery.nack(false);
						}
					});
					log.info("Batch END");
				}, t -> log.error("Err", t));
	}
}
