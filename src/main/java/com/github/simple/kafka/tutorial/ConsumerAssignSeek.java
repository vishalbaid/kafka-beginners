package com.github.simple.kafka.tutorial;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerAssignSeek {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		TopicPartition partitionToReadFrom = new TopicPartition("MyFourthTopic", 1);
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		Long offsetToReadFrom = 15L;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		boolean keepOnReading = true;
		int numberOfMessageToRead = 5;
		int numberOfMessageReadSooFar = 0;
	
		
		while(keepOnReading) {
			ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1000));
			for(ConsumerRecord<String, String> rec : recs){
				System.out.println(rec.key()+" "+rec.value()+" "+rec.partition()+" "+rec.offset());
				numberOfMessageReadSooFar+=1;
				if(numberOfMessageReadSooFar>=numberOfMessageToRead)
				{
					keepOnReading = false;
					break;
				}
			}
		}
		System.out.println("System Exited.");
	}

}
