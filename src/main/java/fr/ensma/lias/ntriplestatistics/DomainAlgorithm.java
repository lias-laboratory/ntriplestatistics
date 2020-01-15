package fr.ensma.lias.ntriplestatistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class DomainAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<String, String> finalResult;

	public static final String TYPE_PREDICATE_IDENTIFIER_DEFAULT = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	
    public static String RDFS_PROPERTYDOMAIN = "http://www.w3.org/2000/01/rdf-schema#domain";

	private DomainAlgorithm(String inputFiles, String outputDirectory) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;
		
		SparkConf conf = new SparkConf().setAppName("domain").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		// Split each line.
		JavaRDD<String[]> rows = sc.textFile(inputFiles).map(line -> line.split(" "));

		// Keep only triples of domain definition. Map subject(predicate name) and object(domain name) Reduce by subject.
		JavaPairRDD<String, String> mapToPairDomain = rows.filter(s->(s[1].equals("<"+RDFS_PROPERTYDOMAIN+">")))
				.mapToPair(s -> new Tuple2<String, String>(s[0], s[2]))
				.reduceByKey((x,y)->x+","+y);

		finalResult = mapToPairDomain;
	}

	private void saveAsTextFile() {
		finalResult.coalesce(1).saveAsTextFile(outputDirectory + "/domain");

		sc.close();
	}

	private String saveAsText() {
		StringBuffer newStringBuffer = new StringBuffer();

		List<Tuple2<String, String>> collect = finalResult.collect();
		for (Tuple2<String, String> tuple2 : collect) {
			newStringBuffer.append(tuple2._1 + "," + tuple2._2 + "\n");
		}

		sc.close();
		return newStringBuffer.toString();
	}

	private Map<String, Cardinality> saveAsMap() {
		Map<String, Cardinality> result = new HashMap<>();
		return result;
	}

	public static class DomainAlgorithmBuilder implements ICardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		public DomainAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		@Override
		public ICardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}
		
		@Override
		public ICardinalityAlgorithmBuilder withTypePredicateIdentifier(String typePredicateIdentifier) {
			return this;
		}

		private DomainAlgorithm build() {
			DomainAlgorithm currentInstance = new DomainAlgorithm(inputFiles, outputDirectory);
			currentInstance.build();

			return currentInstance;
		}

		@Override
		public String buildAsText() {
			DomainAlgorithm build = this.build();
			
			return build.saveAsText();
		}

		@Override
		public void buildAsTextFile() {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			DomainAlgorithm build = this.build();
			build.saveAsTextFile();
		}

		@Override
		public Map<String, Cardinality> buildAsMap() {
			DomainAlgorithm build = this.build();
			
			return build.saveAsMap();
		}
	}

}
