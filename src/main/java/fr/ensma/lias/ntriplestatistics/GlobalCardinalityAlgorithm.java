package fr.ensma.lias.ntriplestatistics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Mickael BARON
 */
public class GlobalCardinalityAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<String, String> finalResult;

	private GlobalCardinalityAlgorithm(String inputFiles, String outputDirectory) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;

		SparkConf conf = new SparkConf().setAppName("global_cardinality").setMaster("local[16]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		JavaRDD<String> textFile = sc.textFile(inputFiles);

		Long subjectNumber = textFile.filter(t -> GlobalCardinalityAlgorithm.getLineFilter(t))
				.map(line -> line.split(" ")[0]).distinct().count();

		// Construct key = (Predicat + Subject)
		JavaPairRDD<String, Long> rdd = textFile.filter(t -> GlobalCardinalityAlgorithm.getLineFilter(t))
				.map(line -> line.split(" ")).mapToPair(s -> new Tuple2<String, Long>(s[1] + " " + s[0], 1L))
				.reduceByKey((x, y) -> x + y).cache();

		// Predicate Max
		JavaPairRDD<String, Long> max = rdd.mapToPair(s -> new Tuple2<String, Long>(s._1.split(" ")[0], s._2))
				.reduceByKey((x, y) -> Math.max(x, y));
		// Predicate Min
		JavaPairRDD<String, Long> min = rdd.mapToPair(s -> new Tuple2<String, Long>(s._1.split(" ")[0], s._2))
				.groupByKey().mapToPair(t -> new Tuple2<String, Long>(t._1, compute(t._2, subjectNumber)));

		// Union of previous RDD.
		this.finalResult = max.union(min).groupByKey().mapToPair(t -> new Tuple2<String, String>(t._1, sort(t._2)));
	}

	private void saveAsTextFile() {
		finalResult.coalesce(1).saveAsTextFile(outputDirectory + "/cardinalities.nt");

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

		List<Tuple2<String, String>> collect = finalResult.collect();
		for (Tuple2<String, String> tuple2 : collect) {
			String[] split = tuple2._2.split(",");
			Cardinality newCardinality = new Cardinality(Long.parseLong(split[0]), Long.parseLong(split[1]));
			result.put(tuple2._1, newCardinality);
		}

		sc.close();
		return result;
	}

	protected static boolean getLineFilter(String line) {
		return !line.startsWith("_");
	}

	private static String sort(Iterable<Long> values) {
		Iterator<Long> iterator = values.iterator();
		Long first = iterator.next();
		Long second = iterator.next();

		if (first <= second) {
			return first + "," + second;
		} else {
			return second + "," + first;
		}
	}

	private static Long compute(Iterable<Long> values, Long subjectNumber) {
		int counter = 0;
		Long min = null;
		for (Long current : values) {
			if (min == null) {
				min = current;
			}
			counter++;
			min = Math.min(min, current);
		}

		if (counter != subjectNumber) {
			return 0L;
		} else {
			return min;
		}
	}

	public static class GlobalCardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		public GlobalCardinalityAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		public GlobalCardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}

		private GlobalCardinalityAlgorithm build() {
			GlobalCardinalityAlgorithm currentInstance = new GlobalCardinalityAlgorithm(inputFiles, outputDirectory);
			currentInstance.build();

			return currentInstance;
		}

		public String buildAsText() {
			GlobalCardinalityAlgorithm build = this.build();
			return build.saveAsText();
		}

		public void buildAsTextFile() {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			GlobalCardinalityAlgorithm build = this.build();
			build.saveAsTextFile();
		}

		public Map<String, Cardinality> buildAsMap() {
			GlobalCardinalityAlgorithm build = this.build();
			return build.saveAsMap();
		}
	}
}
