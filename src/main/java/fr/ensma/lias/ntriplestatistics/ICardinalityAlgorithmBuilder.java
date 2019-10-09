package fr.ensma.lias.ntriplestatistics;

import java.util.Map;

/**
 * @author Mickael BARON
 */
public interface ICardinalityAlgorithmBuilder {

	String buildAsText();
	
	void buildAsTextFile();
	
	Map<String, Cardinality> buildAsMap();
	
	ICardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory);
	
	ICardinalityAlgorithmBuilder withTypePredicateIdentifier(String typePredicateIdentifier);
}
