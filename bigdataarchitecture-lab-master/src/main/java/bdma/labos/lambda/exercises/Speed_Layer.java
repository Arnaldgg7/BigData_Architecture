package bdma.labos.lambda.exercises;

import bdma.labos.lambda.writers.WriterClient;
import com.clearspring.analytics.util.Lists;
import com.mongodb.spark.MongoSpark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import bdma.labos.lambda.utils.Utils;
import bdma.labos.lambda.writers.WriterServer;
import org.bson.Document;
import twitter4j.JSONObject;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Speed_Layer {

	@SuppressWarnings("serial")
	public static void run(String twitterFile) throws Exception {
		// Creating and starting up the writer server for HDFS
		WriterServer writerServer = new WriterServer();
		writerServer.start();
		
		//Including MongoDB connector info into Spark Session configuration (twitter: MongoDB database;   twitter_summary: MongoDB collection)
		// see more info at: https://docs.mongodb.com/spark-connector/master/java-api/
		SparkSession spark = SparkSession.builder()
			      .master("spark://master:7077")
			      .appName("LambdaArchitecture")
			      .config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_summary")
			      .config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_summary")
			      .getOrCreate();
		
		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		 
		// Setting up the Spark streaming context and a batch duration (sliding window) 
		JavaStreamingContext streamContext = new JavaStreamingContext(context, new Duration(1000));
		
		// Creating a Kafka stream for twitter feeds 
		JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = Utils.getKafkaStream(streamContext, twitterFile);
		
		/*********************/					
		// insert your code here
		JavaDStream<Document> documents = kafkaStream.map(t -> {
			JSONObject json = new JSONObject(t.value());
			// Just getting 1 hashtag per tweet:
			Pattern pattern = Pattern.compile(".*?\\s(#\\w+).*?");
			Matcher matcher = pattern.matcher(json.getString("text"));

			String hashtag;
			if (matcher.matches()){
				hashtag = matcher.group(1);
			}
			else {
				hashtag = "";
			}

			// Creating MongoDB Document:
			Document document = new Document();
			document.put("_id", json.getString("id")); // Using the indexed MongoDB '_id' field.
			document.put("hashtag", hashtag);
			document.put("time", json.getString("created"));

			return document;
		}).filter(t -> !t.getString("hashtag").isEmpty());

		documents.print();

		// Saving them to MongoDB, with the specified configuration and the required fields:
		documents.foreachRDD(doc -> {
			MongoSpark.save(doc);
		});

		// Saving the raw tweet in HDFS as well:
		kafkaStream.mapPartitions(part -> {
			List<JSONObject> jsonDocs = Lists.newArrayList();
			while (part.hasNext()){
				jsonDocs.add(new JSONObject(part.next().value()));
			}
			return jsonDocs.iterator();
		}).foreachRDD(rdd -> {
			rdd.foreachPartition(jsonDoc -> {
				// Creation of the writer and opening of the connection:
				WriterClient writer = new WriterClient();
				// Search for hashtags within the tweet's text:
				Pattern pattern = Pattern.compile(".*?\\s(#\\w+).*?");
				while (jsonDoc.hasNext()){
					JSONObject doc = jsonDoc.next();
					// Just writing it to HDFS in case there is a hashtag, since they are the ones that
					// will be interested in to process in batch later on:
					if (pattern.matcher(doc.getString("text")).matches()){
						writer.write((doc.toString()+"\n").getBytes());
					}
				}
				// Closing the connection from HDFS:
				writer.close();
			});
		});

		/*********************/

		streamContext.start();
		streamContext.awaitTermination();
		
		writerServer.finish();
	}
}
