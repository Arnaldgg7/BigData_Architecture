package bdma.labos.lambda;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;

import bdma.labos.lambda.exercises.Tweet_Stream;
import bdma.labos.lambda.exercises.Speed_Layer;
import bdma.labos.lambda.exercises.Batch_Layer;
import bdma.labos.lambda.exercises.Serving_Layer;

public class Lambda {

	// IMPORTANT: modify to your bdma user: e.g., bdma00
	private static String TWITTER_CONFIG_PATH = "C:\\Users\\Arnald\\Desktop\\ARNALD\\KNOWLEDGE\\PROJECTE MASTER\\MASTER\\Hands-On Experience\\Big Data Architecture\\bigdataarchitecture-lab-master\\bigdataarchitecture-lab-master\\src\\main\\resources\\twitter.txt";
	
	public static void main(String[] args) {
		
	
		LogManager.getRootLogger().setLevel(Level.ERROR);
		try {
			if (args[0].equals("-exercise1")) {
				Tweet_Stream.run(TWITTER_CONFIG_PATH);
			} else if (args[0].equals("-exercise1_speed")) {
				Speed_Layer.run(TWITTER_CONFIG_PATH);
			} else if (args[0].equals("-exercise2_batch")) {
				Batch_Layer.run();
			} else if (args[0].equals("-exercise3_serving")) {
				Serving_Layer.run(args[1]);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
