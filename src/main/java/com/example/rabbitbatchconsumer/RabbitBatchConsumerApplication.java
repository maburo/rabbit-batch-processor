package com.example.rabbitbatchconsumer;

import com.rabbitmq.client.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.Disposable;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

@Slf4j
@EnableScheduling
@SpringBootApplication
public class RabbitBatchConsumerApplication {

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class Message {
		private long deliveryTag;
		private Channel channel;
		private String payload;
	}

	public static void main(String[] args) {
		SpringApplication.run(RabbitBatchConsumerApplication.class, args);
	}

	private static final String QUEUE_NAME = "batch_queue";

	@Autowired
	private FluxSink<Message> sink;

	@Autowired
	private UnicastProcessor<Message> processor;

	@Autowired
	private RabbitTemplate rabbitTemplate;

	@Bean
	public UnicastProcessor<Message> publisher() {
		return UnicastProcessor.create();
	}

	@Bean
	public FluxSink<Message> sink(UnicastProcessor<Message> processor) {
		return processor.sink();
	}

	@Bean
	public Queue batchQueue() {
		return new Queue(QUEUE_NAME, true);
	}

	@Scheduled(fixedDelay = 1000)
	public void sendMessage() {
		System.out.println("send...");
		rabbitTemplate.convertAndSend("batch_queue", System.currentTimeMillis());
	}

	@RabbitListener(queues = "batch_queue", ackMode = "MANUAL")
	public void onMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
		System.out.println("onMessage...");
		sink.next(new Message(tag, channel, message));
	}

	@Bean
	public Disposable subscriber() {
		return processor.bufferTimeout(10, Duration.of(5, ChronoUnit.SECONDS))
				.subscribe(list -> {
					System.out.println("Batch processing...");
					list.forEach(this::acknowledge);
					System.out.println("Batch complete");
				}, t -> log.error("Error", t));
	}

	private void acknowledge(Message message) {
		try {
			System.out.println(message.getPayload());
			message.channel.basicAck(message.getDeliveryTag(), false);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
