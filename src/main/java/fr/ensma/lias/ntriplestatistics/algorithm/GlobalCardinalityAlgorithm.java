package fr.ensma.lias.ntriplestatistics.algorithm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import fr.ensma.lias.ntriplestatistics.model.Cardinality;
import scala.Tuple2;

/**
 * @author Mickael BARON (baron@ensma.fr)
 */
public class GlobalCardinalityAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<String, String> finalResult;
	private JavaPairRDD<String, Long> maxResult;

	private static String separatorIdentifier;
	
	private GlobalCardinalityAlgorithm(String inputFiles, String outputDirectory, String separatorIdentifier) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;
		GlobalCardinalityAlgorithm.separatorIdentifier = separatorIdentifier;

		SparkConf conf = new SparkConf().setAppName("global_cardinality").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		JavaRDD<String> textFile = sc.textFile(inputFiles);

		Long subjectNumber = textFile.map(line -> line.split(separatorIdentifier)[0]).distinct().count();

		// Construct key = (Predicat + Subject)
		JavaPairRDD<String, Long> rdd = textFile.map(line -> line.split(separatorIdentifier)).mapToPair(s -> new Tuple2<String, Long>(s[1] + " " + s[0], 1L))
				.reduceByKey((x, y) -> x + y).cache();

		// Predicate Max
		JavaPairRDD<String, Long> max = rdd.mapToPair(s -> new Tuple2<String, Long>(s._1.split(" ")[0], s._2))
				.reduceByKey((x, y) -> Math.max(x, y));
		// Predicate Min
		JavaPairRDD<String, Long> min = rdd.mapToPair(s -> new Tuple2<String, Long>(s._1.split(" ")[0], s._2))
				.groupByKey().mapToPair(t -> new Tuple2<String, Long>(t._1, compute(t._2, subjectNumber)));

		// Union of previous RDD.
		this.finalResult = max.union(min).groupByKey().mapToPair(t -> new Tuple2<String, String>(t._1, sort(t._2)));
		
		this.maxResult=max.mapToPair(s -> new Tuple2<String, Long>(getNiceName(s._1),s._2));
	}
	
	private void saveMaxAsFile() throws IOException {
		//StringBuffer newStringBuffer = new StringBuffer();
		File OUTPUT_FILE = new File("global_card.txt");
		FileWriter res = new FileWriter(OUTPUT_FILE);
		BufferedWriter bw = new BufferedWriter(res);
		List<Tuple2<String, Long>> collect = maxResult.collect();
		for (Tuple2<String, Long> tuple2 : collect) {
			//newStringBuffer.append(tuple2._1 + ":" + tuple2._2 + "\n");
			bw.write(tuple2._1 + ":" + tuple2._2);
			bw.newLine();
		}
		bw.close();
		sc.close();
	}

	private void saveAsTextFile() {
		finalResult.coalesce(1).saveAsTextFile(outputDirectory + "/globalcardinalities");

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
	private static String getNiceName(String uri) {
		int indexOfSeparator = uri.lastIndexOf(":");
		int indexOfEnd = uri.lastIndexOf(">");
		if (indexOfEnd==-1)
			indexOfEnd=uri.length();
		if (indexOfSeparator != -1) {
			return uri.substring(indexOfSeparator + 1, indexOfEnd);
		} else {
			return uri;
		}
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

	public static class GlobalCardinalityAlgorithmBuilder implements ICardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		private String separatorIdentifier = ICardinalityAlgorithmBuilder.DEFAULT_SEPARATOR;
		
		public GlobalCardinalityAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		public GlobalCardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}

		private GlobalCardinalityAlgorithm build() {
			GlobalCardinalityAlgorithm currentInstance = new GlobalCardinalityAlgorithm(inputFiles, outputDirectory, separatorIdentifier);
			currentInstance.build();

			return currentInstance;
		}

		@Override
		public String buildAsText() {
			GlobalCardinalityAlgorithm build = this.build();
			return build.saveAsText();
		}

		@Override
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
		
		public void buildMaxAsFile() throws IOException {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			GlobalCardinalityAlgorithm build = this.build();
			build.saveMaxAsFile();
		}

		@Override
		public ICardinalityAlgorithmBuilder withTypePredicateIdentifier(String typePredicateIdentifier) {
			return null;
		}

		@Override
		public ICardinalityAlgorithmBuilder withSeparator(String separator) {
			this.separatorIdentifier = separator;
			
			return this;
		}
	}
}
