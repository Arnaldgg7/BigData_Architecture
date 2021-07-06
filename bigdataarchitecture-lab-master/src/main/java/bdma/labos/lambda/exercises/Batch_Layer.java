package bdma.labos.lambda.exercises;

import com.mongodb.spark.MongoSpark;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import scala.Tuple3;
import twitter4j.JSONObject;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;


public class Batch_Layer {

	// IMPORTANT: Modified with the directory where we stored all raw tweets:
	private static String HDFS = "hdfs://master:27000/user/bdma17/lambda/*";
	
	
	@SuppressWarnings("serial")
	public static void run() throws Exception {
		
		//Including MongoDB connector info into Spark Session configuration (twitter: MongoDB database;   twitter_sentiment: MongoDB collection) 
		// see more info at: https://docs.mongodb.com/spark-connector/master/java-api/
		SparkSession spark = SparkSession.builder()
			      .master("spark://master:7077")
			      .appName("LambdaArchitecture")
			      .config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_sentiment")
			      .config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_sentiment")
			      .getOrCreate();

		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
		
		/*********************/
		//insert your code here

		JavaPairRDD<String, String> eng_tweets = context.textFile(HDFS)
				.map(t -> new JSONObject(t))
				.filter(t -> t.getString("text") != null) // Checking for 'nulls' just in the text, as sometimes happens.
				.filter(t -> LanguageDetector.isEnglish(t.getString("text")))
				.mapToPair(t -> new Tuple2<String, String>(t.getString("id"), t.getString("text")))
				.mapValues(text -> text.replaceAll("[^a-zA-Z\\s]", "").trim().toLowerCase())
				.filter(t -> t._2.length() > 0); // Checking for empty texts after the replacement.

		JavaPairRDD<String, String> stem_tweets = eng_tweets
				.mapValues(t -> {
					List<String> tweet_stop_words = Arrays.stream(t.split("\\s+"))
							.filter(StopWords.getWords()::contains).collect(Collectors.toList());
					for (String word : tweet_stop_words) {
						t = t.replaceAll("\\s+"+word+"\\s+", " ");
					}
					return t;
				})
				// Checking that the removal of the Stop Words has not yielded an empty tweet:
				.filter(t -> t._2.trim().length() > 0);


		JavaPairRDD<Tuple2<String,String>, Float> pos_tweets = stem_tweets.mapToPair(t -> {
			String[] tweet_words = t._2.split("\\s+");
			float n = tweet_words.length;
			float pos_words = Arrays.stream(tweet_words)
					.filter(PositiveWords.getWords()::contains).count();
			return new Tuple2<>(t, pos_words/n);
		});

		JavaPairRDD<Tuple2<String,String>, Float> neg_tweets = stem_tweets.mapToPair(t -> {
			String[] tweet_words = t._2.split("\\s+");
			float n = tweet_words.length;
			float neg_words = Arrays.stream(tweet_words)
					.filter(NegativeWords.getWords()::contains).count();
			return new Tuple2<>(t, neg_words/n);
		});

		JavaRDD<Tuple3<String, String, String>> final_joined =
				pos_tweets.join(neg_tweets).map(t -> {
					String sentiment;
					if (t._2._1 > t._2._2) {
						sentiment = "positive";
					}
					else if (t._2._1 < t._2._2) {
						sentiment = "negative";
					}
					else {
						sentiment = "neutral";
					}
					return new Tuple3<>(t._1._1, t._1._2, sentiment);
				});

		// Creating the final document format to store in the Batch View:
		JavaRDD<Document> documents = final_joined.map(t -> {
			Document json = new Document();
			json.put("_id", t._1()); // Using the indexed MongoDB '_id' field.
			json.put("text", t._2());
			json.put("sentiment", t._3());
			return json;
			});

		// Storing the document in the batch view tool (MongoDB):
		MongoSpark.save(documents);

        
        /*********************/

        context.close();
	}
        
        
}
