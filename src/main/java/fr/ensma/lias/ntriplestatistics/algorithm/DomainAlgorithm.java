package fr.ensma.lias.ntriplestatistics.algorithm;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import fr.ensma.lias.ntriplestatistics.model.Cardinality;
import scala.Tuple2;

/**
 * @author Louise PARKIN (louise.parkin@ensma.fr)
 */
public class DomainAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<String, String> finalResult;

	private static String separatorIdentifier;

	private DomainAlgorithm(String inputFiles, String outputDirectory, String separatorIdentifier) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;
		DomainAlgorithm.separatorIdentifier = separatorIdentifier;

		SparkConf conf = new SparkConf().setAppName("domain").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		// Split each line.
		JavaRDD<String[]> rows = sc.textFile(inputFiles).map(line -> line.split(separatorIdentifier));

		// Keep only triples of domain definition. Map subject(predicate name) and
		// object(domain name) Reduce by subject.
		JavaPairRDD<String, String> mapToPairDomain = rows
				.filter(s -> (s[1].equals("<" + ICardinalityAlgorithmBuilder.RDFS_PROPERTYDOMAIN + ">")))
				.mapToPair(s -> new Tuple2<String, String>(s[0], s[2])).reduceByKey((x, y) -> x + "," + y);

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
		
		sc.close();
		return result;
	}

	public static class DomainAlgorithmBuilder implements ICardinalityAlgorithmBuilder {
		
		private String outputDirectory;

		private String inputFiles;

		private String separatorIdentifier = ICardinalityAlgorithmBuilder.DEFAULT_SEPARATOR;

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
			DomainAlgorithm currentInstance = new DomainAlgorithm(inputFiles, outputDirectory, separatorIdentifier);
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

		public Map<String, Cardinality> buildAsMap() {
			DomainAlgorithm build = this.build();

			return build.saveAsMap();
		}

		@Override
		public ICardinalityAlgorithmBuilder withSeparator(String separator) {
			this.separatorIdentifier = separator;

			return this;
		}
	}

}
