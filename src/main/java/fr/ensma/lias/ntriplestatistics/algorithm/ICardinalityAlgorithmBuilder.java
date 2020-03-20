package fr.ensma.lias.ntriplestatistics.algorithm;

/**
 * @author Mickael BARON (baron@ensma.fr)
 */
public interface ICardinalityAlgorithmBuilder {

	public static final String DEFAULT_SEPARATOR = " ";
	
	public static final String TYPE_PREDICATE_IDENTIFIER_DEFAULT = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	
	public static final String THING = "Thing";
	
	public static String RDFS_PROPERTYDOMAIN = "http://www.w3.org/2000/01/rdf-schema#domain";
	
	public static final String CLASS_DEFINITION_IDENTIFIER = "@";
	
	String buildAsText();
	
	void buildAsTextFile();
	
	ICardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory);
	
	ICardinalityAlgorithmBuilder withTypePredicateIdentifier(String typePredicateIdentifier);
	
	ICardinalityAlgorithmBuilder withSeparator(String separator);
}
