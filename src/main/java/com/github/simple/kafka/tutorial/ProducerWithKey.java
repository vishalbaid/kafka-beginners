package com.github.simple.kafka.tutorial;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerWithKey {

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
				
		for(int i=50;i<=70;i++){
			// Create a ProducerRecords
			ProducerRecord<String, String> rec = new ProducerRecord<String, String>("MyFourthTopic","Key "+Integer.toString(i), "Hello World "+Integer.toString(i));
					
			// Send Data
			producer.send(rec, new Callback() {
				public void onCompletion(RecordMetadata recordMetadata, Exception e) {
					if(e == null) {
						System.out.println(recordMetadata.topic()+" "+recordMetadata.offset()+ " "+recordMetadata.partition());
					}
					else{
						e.printStackTrace();
					}
				}
			});
		}	
				
		// Flush and close the producer
		producer.flush();
		producer.close();

	}

}
