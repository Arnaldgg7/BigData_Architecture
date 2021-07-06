# BigData_Architecture
Implementing a Lambda Big Data Architecture by means of a Speed Layer, a Batch Layer and a Serving Layer.

The present implementation is based on the following layers and Big Data technologies, all of them driven by **Apache Spark** as the cornerstone when it comes to distributed processing:

**1) Speed Layer:** In this layer, we use **Spark Streaming** to get micro-batches of 1 second with all published tweets on Twitter, which are filtered depending on if they contain a hashtag or not. The ones that contain a hashtag are stored in a JSON Document with the keys 'id', 'hashtag' and 'time', storing each documeent in **MongoDB Document Store**. Here, we use the tweet ID as 'id' in MongoDB documents, which entails primary indexing to enable faster retrieval further on.

In addition to this, each tweet that contains a hashtag is stored in the raw format it is received in **HDFS**, so that each of them can be processed in a more complex way further on to get more intricate insights from it.

It is worth-pointing out that the ingestion of tweets from the Twitter API to be consumed and processed by Spark Streaming is performed by means of **Apache Kafka**, whereby we create a 'topic' representing a queue to be consumed by Spark, being it safer, more reliable and distributed-friendly.


**2) Batch Layer:** Once all tweets have been quickly pre-processed, summarized in terms of 'hashtags' in JSON Documents and stored in **MongoDB**, as well as stored in raw form in **HDFS**, it is time now to retrieve them from HDFS and perform more complex processing in the Batch Layer.

In this layer, so, we perform **Sentiment Analysis** by using dictionaries of 'Stop Words', 'Positive Words' and 'Negative Words', thereby getting a grade for each tweet of 'how positive it is' or 'how negative it is' (or neutral, if both grades match), so that we can finally determine the sentiment of each tweet. Once done, the summary of the tweet, this time in terms of 'sentiment' is stored in a different collection in **MongoDB** (with keys of 'id', 'text' and 'sentiment').


**3) Serving Layer:** Finally, we create some sort of 'Query Engine', whereby we provide a hashtag when calling this layer (Example: 'Serving_Layer "#EURO2021"'), and the Spark program performs the following:

    A. It retrieves all JSON Documents that contain the stated hashtag and converts into seconds the 'time' values from all JSON Documents from the MongoDB collection we stored in the Speed Layer.
    
    B. It sorts the retrieved documents by timestamp and gets all 'id' of those documents, searching for these very same 'ids' in the 2nd MongoDB collection with the sentiment analysis we stored in MongoDB in the Batch Layer.
    
    C. One by one, the Spark program checks if the documents (tweets) that are being retrieved from this 2nd MongoDB collection are 'positive' in terms of their sentiment.
      C.1. In case they are, a 'counter' starts, and the program keeps retrieving JSON Documents by 'id' in such a 2nd MongoDB collection. The program is going to keep retrieving JSON Documents from this 2nd MongoDB collection until it retrieves a tweet that is not 'positive'.
      C.2. Once that happens, the 'counter' stops and the program displays:
        - The hashtag you stated in the query when calling the program.
        - The number of tweets between a 'positive' and a 'non-positive' tweet for that hashtag.
        - The seconds that have passed between a 'positive' and a 'non-positive' tweet for such a hashtag.


Overall, **it is a simple but tangible implementation of a Big Data Architecture, whereby we use different Big Data technologies, each of them with different purposes depending on the layer we put them, in order to achieve a concrete task** (getting the number of tweets and the seconds in between for a given hashtag from a 'positive' tweet and a 'non-positive' tweet) **that may entail dealing with huge volumes of unstructured data with high ingestion speed.**
