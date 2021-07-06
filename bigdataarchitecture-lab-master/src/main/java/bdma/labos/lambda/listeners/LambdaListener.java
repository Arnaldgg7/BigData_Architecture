package bdma.labos.lambda.listeners;

import twitter4j.JSONException;
import twitter4j.JSONObject;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class LambdaListener implements StatusListener {
	
	private static final Properties KAFKA_CONFIG;
	private static final String TOPIC = "twitter";
	static {
		KAFKA_CONFIG = new Properties();
		KAFKA_CONFIG.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "slave1:9092");
		KAFKA_CONFIG.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		KAFKA_CONFIG.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
	}
	
	public KafkaProducer<String, String> producer;
	
	public LambdaListener() {
		this.producer = new KafkaProducer<String, String>(KAFKA_CONFIG);
	}
	
	public void onStatus(Status tweet) {
		JSONObject json = new JSONObject();
		try {
			json.put("id", tweet.getId());
			json.put("text", tweet.getText().replaceAll("\n", " ").replaceAll(":", ""));
			json.put("created", tweet.getCreatedAt().toString().replaceAll("\n", " ").replaceAll(":", ""));
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ProducerRecord<String, String> message = 
				new ProducerRecord<String, String>(TOPIC, String.valueOf(tweet.getId()), 
						json.toString());
		this.producer.send(message);
    }

	public void onException(Exception exception) {
		return;
	}

	public void onDeletionNotice(StatusDeletionNotice arg0) {
		return;
	}

	public void onScrubGeo(long arg0, long arg1) {
		return;
	}

	public void onStallWarning(StallWarning arg0) {
		return;
	}

	public void onTrackLimitationNotice(int arg0) {
		return;
	}

}
