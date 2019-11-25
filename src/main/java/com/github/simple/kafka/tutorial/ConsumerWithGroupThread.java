package com.github.simple.kafka.tutorial;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerWithGroupThread {

	public static void main(String[] args) throws IOException {
		new ConsumerWithGroupThread().run();
	}
	
	public void run() throws IOException {
		final CountDownLatch latch = new CountDownLatch(1); 
		final Runnable myConsumerRunnable = new ConsumerRunnable(latch); 
		
		Thread myConsumerThread = new Thread(myConsumerRunnable);
		myConsumerThread.start();
		
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					System.out.println("Caught Shutdown Hook.");
					((ConsumerRunnable)myConsumerRunnable).shutdown();
					try {
						latch.await();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.print("System exited");
				}
		});
		System.in.read();
		System.exit(0);
		try {
			latch.await();
		} catch (InterruptedException e) {
			System.out.println("Application got Interuppted.");
			e.printStackTrace();
		}
	}
	
	public class ConsumerRunnable implements Runnable {
		
		private CountDownLatch latch;
		private KafkaConsumer<String, String> consumer;
		
		public ConsumerRunnable(CountDownLatch latch) {
			this.latch = latch;
			Properties props = new Properties();
			props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
			props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "MyFirstApplication");
			props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
			this.consumer = new KafkaConsumer<String, String>(props);
			consumer.subscribe(Arrays.asList("MyFourthTopic"));
		}

		public void run() {
			try {
				while(true) {
					ConsumerRecords<String, String> recs = consumer.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> rec : recs){
						System.out.println(rec.key()+" "+rec.value()+" "+rec.partition()+" "+rec.offset());
					}
				}
			}catch(WakeupException e) {
				System.out.println("Receive ShutDown signal.");
				e.printStackTrace();
			}
			finally {
				consumer.close();
				// tell our main coe we are done with the consumer
				latch.countDown();
			}
		}
		
		public void shutdown() {
			// the wakeup method is the special method to interrupt consumer.poll()
			// it will throw the excpetion WakeUpException
			System.out.println("In ShutDown");
			consumer.wakeup();
		}
		
	}
}
