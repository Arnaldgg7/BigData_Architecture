package bdma.labos.lambda.exercises;

import org.apache.tika.language.LanguageIdentifier;

public class LanguageDetector {

	static boolean isEnglish(String s) {
		LanguageIdentifier identifier = new LanguageIdentifier(s);
		return identifier.getLanguage().equals("en");
	}
	
}
