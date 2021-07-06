package bdma.labos.lambda.exercises;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.spark_project.guava.collect.Lists;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class Serving_Layer {

	public static void run(String hashtag) throws Exception {

		/*********************/
		//insert your code here 
		SparkSession spark = SparkSession.builder()
				.master("spark://master:7077")
				.appName("LambdaArchitecture")
				// We define a default MongoDB collection that we can further on overwrite:
				.config("spark.mongodb.input.uri", "mongodb://master:27017/twitter.twitter_sentiment")
				.config("spark.mongodb.output.uri", "mongodb://master:27017/twitter.twitter_sentiment")
				.getOrCreate();

		JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

		// We create a Custom Read Configuration for each collection:
		// 'twitter_summary' reading preference:
		Map<String, String> readOverrides = new HashMap<String, String>();
		readOverrides.put("uri", "mongodb://master:27017/");
		readOverrides.put("database", "twitter");
		readOverrides.put("collection", "twitter_summary");
		readOverrides.put("readPreference.name", "secondaryPreferred");
		ReadConfig twitter_summary = ReadConfig.create(context).withOptions(readOverrides);

		// Loading data from the 'twitter_summary' collection:
		Dataset<Row> summaryDF = MongoSpark.load(context, twitter_summary).toDF();

		// 'twitter_sentiment' reading preference:
		Map<String, String> readOverrides2 = new HashMap<String, String>();
		readOverrides.put("uri", "mongodb://master:27017/");
		readOverrides.put("database", "twitter");
		readOverrides2.put("collection", "twitter_sentiment");
		readOverrides2.put("readPreference.name", "secondaryPreferred");
		ReadConfig twitter_sentiment = ReadConfig.create(context).withOptions(readOverrides2);

		// Loading data from the 'twitter_sentiment' collection:
		Dataset<Row> sentimentDF = MongoSpark.load(context, twitter_sentiment).toDF();


		// Searching the given hashtag:
		summaryDF.createOrReplaceTempView("twitter_summary");
		sentimentDF.createOrReplaceTempView("twitter_sentiment");

		// We define a User-Defined Function to deal with the data format we got from Twitter API and we
		// have stored in MongoDB, to convert it into 'timestamp' seconds since 1970, like in Unix:
		spark.sqlContext().udf().register("toTimestamp", (UDF1<String, Long>) (colVal) -> {
			SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HHmmss zzzz yyyy", Locale.US);
			Long timestamp = 0l;
			try {
				timestamp = formatter.parse(colVal).getTime() / 1000; // To get seconds.
			} catch (ParseException e) {
				e.printStackTrace();
			}
			return timestamp;

		}, DataTypes.LongType);

		// We apply the previous defined function to the Spark DataFrame and we sort by such timestamp value, to
		// get from the very first tweets up to the most recent ones:
		Dataset<Row> summaryDataset2 = summaryDF.withColumn("timestamp",
				functions.callUDF("toTimestamp", functions.col("time"))).drop("time").sort("timestamp");

		summaryDataset2.createOrReplaceTempView("twitter_summary2");

		Dataset<Row> hashtagResult = spark.sqlContext().sql("SELECT _id, hashtag, timestamp FROM twitter_summary2 WHERE hashtag=" +
				"'"+hashtag+"'");

		hashtagResult.show();

		if (hashtagResult.count() == 0){
			System.out.println("There is no hashtag '"+hashtag+"' in the database.");
			System.exit(0);
		}

		long seconds = 0;
		int tweets = 0;
		boolean interval = false;
		List<HashMap<String, String>> results = Lists.newArrayList();
		// We use 'toLocalIterator()' function to get 1 partition at a time and avoid memory issues:
		Iterator<Row> summaryDocs = hashtagResult.toLocalIterator();
		while (summaryDocs.hasNext()) {
			Row summaryDoc = summaryDocs.next();
			String id = summaryDoc.getString(0);
			long timestamp = summaryDoc.getLong(2);

			// Notice that if the tweet corresponds is written in other language than 'English', the pre-processing
			// we made to extract the sentiment was not applied and the tweet was discarded, so some 'empty'
			// datasets might be returned as a result of the following query, which are filtered further on:
			Dataset<Row> sentimentDocs = sentimentDF.sqlContext().sql("SELECT _id, text, sentiment FROM twitter_sentiment WHERE _id="
					+ "'" + id + "'");

			if (interval == true && sentimentDocs.count()!=0){
				Row sentimentDoc = sentimentDocs.collectAsList().get(0); // Each Tweet ID is unique, so it is going to return just 1 Row.
				if (!sentimentDoc.getString(2).equals("positive")){
					// It means 'end of the current interval', so we store the output to be printed later on:
					tweets ++;
					seconds = (timestamp - seconds);  // Getting the number of seconds.
					HashMap<String, String> output = new HashMap<>();
					output.put("hashtag", hashtag);
					output.put("seconds", String.valueOf(seconds));
					output.put("tweets", String.valueOf(tweets));
					results.add(output);

					// Resetting the variables to keep looping over the rest of the hashtag tweets and get more intervals:
					interval = false;
					tweets = 0;
				}

				else {
					// Here means that the retrieved document is positive, so we just add '1' to tweets and keep looping:
					tweets++;
				}
			}

			else if (interval == false && sentimentDocs.count()!=0){
				Row sentimentDoc = sentimentDocs.collectAsList().get(0); // Each Tweet ID is unique, so it is going to return just 1 Row.
				if (sentimentDoc.getString(2).equals("positive")){
					// It means 'start of the interval', so we set the corresponding variables:
					seconds = timestamp;
					interval = true;
				}
			}
		}

		if (results.size()>0){
			for (int i=0; i<results.size(); i++){
				HashMap<String, String> output = results.get(i);
				System.out.println("\nHashtag:   " + output.get("hashtag") + "\n" + "Interval between a positive tweet and a neutral/negative " +
						"tweet:   " + output.get("seconds") + " seconds" + "\n" + "Number of tweets within the interval:   " + output.get("tweets") + " tweets");
			}
		}
		else{
			System.out.println("There are no intervals from positive tweets up to neutral/negative tweets for this hashtag.");
		}

		// We need to close the context to delete the Temporary Files created by Spark:
		context.close();

			/*********************/

	}
}
