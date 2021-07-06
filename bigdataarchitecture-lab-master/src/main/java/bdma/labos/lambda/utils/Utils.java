package bdma.labos.lambda.utils;

import java.io.FileInputStream;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import bdma.labos.lambda.listeners.LambdaListener;

public class Utils {
	
	public static JavaInputDStream<ConsumerRecord<String, String>> getKafkaStream(JavaStreamingContext streamContext, String twitterFile) throws Exception {
		Utils.setupTwitter(twitterFile);
		TwitterStream twitterStream = new TwitterStreamFactory().getInstance();
	    twitterStream.addListener(new LambdaListener());
	    twitterStream.sample();
	    
	    Collection<String> topics = Arrays.asList("twitter");

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put("bootstrap.servers", "slave1:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "twitter-consumer-group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);


		final JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
			    streamContext,
			    LocationStrategies.PreferConsistent(),
			    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
			  );
				
		return kafkaStream;
	}

	public static void setupTwitter(String path) throws Exception {
		Properties props = new Properties();
		props.load(new FileInputStream(path));

		String consumerKey = props.getProperty("consumerKey");
		String consumerSecret = props.getProperty("consumerSecret");
		String accessToken = props.getProperty("accessToken");
		String accessTokenSecret = props.getProperty("accessTokenSecret");

		configureTwitterCredentials(consumerKey, consumerSecret, accessToken, accessTokenSecret);
	}

	public static void configureTwitterCredentials(String apiKey, String apiSecret,
			String accessToken, String accessTokenSecret) throws Exception {
		HashMap<String, String> configs = new HashMap<String, String>();
		configs.put("apiKey", apiKey);
		configs.put("apiSecret", apiSecret);
		configs.put("accessToken", accessToken);
		configs.put("accessTokenSecret", accessTokenSecret);
		Object[] keys = configs.keySet().toArray();
		for (int k = 0; k < keys.length; k++) {
			String key = keys[k].toString();
			String value = configs.get(key).trim();
			if (value.isEmpty()) {
				throw new Exception("Error setting authentication - value for "
						+ key + " not set");
			}
			String fullKey = "twitter4j.oauth."
					+ key.replace("api", "consumer");
			System.setProperty(fullKey, value);
			System.out.println("\tProperty " + key + " set as [" + value + "]");
		}
		System.out.println();
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void setEnv(Map<String, String> newenv) {
		try {
	        Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
	        Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
	        theEnvironmentField.setAccessible(true);
	        Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
	        env.putAll(newenv);
	        Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
	        theCaseInsensitiveEnvironmentField.setAccessible(true);
	        Map<String, String> cienv = (Map<String, String>)     theCaseInsensitiveEnvironmentField.get(null);
	        cienv.putAll(newenv);
	    }
	    catch (NoSuchFieldException e) {
	      try {
	        Class[] classes = Collections.class.getDeclaredClasses();
	        Map<String, String> env = System.getenv();
	        for(Class cl : classes) {
	            if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
	                Field field = cl.getDeclaredField("m");
	                field.setAccessible(true);
	                Object obj = field.get(env);
	                Map<String, String> map = (Map<String, String>) obj;
	                map.clear();
	                map.putAll(newenv);
	            }
	        }
	      } catch (Exception e2) {
	        e2.printStackTrace();
	      }
	    } catch (Exception e1) {
	        e1.printStackTrace();
	    } 
	}

}