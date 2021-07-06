package bdma.labos.lambda.exercises;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import bdma.labos.lambda.utils.Utils;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import org.apache.spark.api.java.function.Function;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Map;
import twitter4j.JSONObject;

import java.util.Arrays;
import java.util.Collection;

public class Tweet_Stream {

	@SuppressWarnings("serial")
	public static void run(String twitterFile) throws Exception {
		SparkConf conf = new SparkConf().setAppName("LambdaArchitecture").setMaster("spark://master:7077");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaStreamingContext streamContext = new JavaStreamingContext(context, new Duration(1000));
		
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = Utils.getKafkaStream(streamContext, twitterFile);
		/*********************/
		//insert your code here
		kafkaStream.map(t -> {
			JSONObject json = new JSONObject(t.value());
			String text = json.getString("text");
			return text;
		}).print();


		/********************/
		
		streamContext.start();
		streamContext.awaitTermination();
	}
	
}
