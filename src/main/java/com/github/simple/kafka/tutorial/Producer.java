package com.github.simple.kafka.tutorial;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		// Create Producer Props
		Properties props = new Properties();
		/*props.setProperty("bootstrap-server", "localhost:9092");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName() );*/
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName() );

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		
		// Crate a ProducerRecords
		ProducerRecord<String, String> rec = new ProducerRecord<String, String>("first_topic", "Hello World 1 !");
		
		// Send Data
		Future<RecordMetadata> future = producer.send(rec);
		System.out.println(future.get().toString());
		
		// Flush and close the producer
		producer.flush();
		producer.close();
	}

}
