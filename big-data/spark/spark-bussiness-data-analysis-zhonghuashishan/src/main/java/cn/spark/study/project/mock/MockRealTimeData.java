package cn.spark.study.project.mock;

import cn.spark.study.project.conf.ConfigurationManager;
import cn.spark.study.project.constant.Constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;



public class MockRealTimeData extends Thread {
	
	private static final Random random = new Random();
	private static final String[] provinces = new String[]{"Jiangsu", "Hubei", "Hunan", "Henan", "Hebei"};  
	private static final Map<String, String[]> provinceCityMap = new HashMap<String, String[]>();
	private static final String topic = ConfigurationManager.getProperty(Constants.KAFKA_TOPIC);
	
	private KafkaProducer<String, String> producer;
	
	public MockRealTimeData() {
		provinceCityMap.put("Jiangsu", new String[] {"Nanjing", "Suzhou"});  
		provinceCityMap.put("Hubei", new String[] {"Wuhan", "Jingzhou"});
		provinceCityMap.put("Hunan", new String[] {"Changsha", "Xiangtan"});
		provinceCityMap.put("Henan", new String[] {"Zhengzhou", "Luoyang"});
		provinceCityMap.put("Hebei", new String[] {"Shijiazhuang", "Tangshan"});  
		
		producer = new KafkaProducer<String, String>(createProducerProperties());
	}
	
	private Properties createProducerProperties() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,ConfigurationManager.getProperty(Constants.KAFKA_KEY_SERIALIZER));
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG ,ConfigurationManager.getProperty(Constants.KAFKA_VALUE_SERIALIZER));
		return  props;
	}
	
	public void run() {
		while(true) {	
			String province = provinces[random.nextInt(5)];  
			String city = provinceCityMap.get(province)[random.nextInt(2)];
			
			String log = new Date().getTime() + " " + province + " " + city + " " 
					+ random.nextInt(1000) + " " + random.nextInt(10);
			ProducerRecord<String, String> adRealTimeLogRecord = new ProducerRecord<>(topic, "AdRealTimeLog", log);
			producer.send(adRealTimeLogRecord);
			
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}  
		}
	}
	
	/**
	 * 启动Kafka Producer
	 * @param args
	 */
	public static void main(String[] args) {
		MockRealTimeData producer = new MockRealTimeData();
		producer.start();
	}
	
}
