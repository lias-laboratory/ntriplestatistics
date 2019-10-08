package fr.ensma.lias.ntriplestatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.spark_project.guava.collect.Iterables;

import scala.Tuple2;

/**
 * @author Mickael BARON
 */
public class LocalCardinalityAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<String, String> finalResult;

	private static final String NO_CLASS_DEFINITION = "VOID";

	private static final String CLASS_DEFINITION_IDENTIFIER = "@";

	private static final String TYPE_PREDICATE_NAME = "type";

	private LocalCardinalityAlgorithm(String inputFiles, String outputDirectory) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;

		SparkConf conf = new SparkConf().setAppName("local_cardinality").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		// Split each line.
		JavaRDD<String[]> rows = sc.textFile(inputFiles).map(line -> line.split(" "));

		// Eliminate object and for 'type class' couple transform to @class.
		JavaPairRDD<String, String> mapToPairSubjects = rows
				.mapToPair(s -> new Tuple2<String, String>(s[0], simplifyPredicateObject(s[1], s[2])));
		mapToPairSubjects.foreach(t -> System.out.println(t));

		// Group by subject.
		JavaPairRDD<String, Iterable<String>> groupBySubject = mapToPairSubjects.groupByKey();
		groupBySubject.foreach(t -> System.out.println(t));

		// Change main key by @class (subjects are no longer useful). Count the
		// predicates.
		JavaPairRDD<String, Iterable<String>> mapToPairPredicats = groupBySubject.mapToPair(t -> extractKeys(t._2));
		mapToPairPredicats.foreach(t -> System.out.println(t));
	
		// Reduce by @class and remove predicates without defined class.
		JavaPairRDD<String, Iterable<String>> reduceByKey = mapToPairPredicats
				.filter(t -> !NO_CLASS_DEFINITION.equals(t._1)).reduceByKey((x, y) -> merge(x, y));
		reduceByKey.foreach(t -> System.out.println(t));
		
		// Create a couple key from @class and predicate.
		JavaPairRDD<String, Long> flatMapToPair = reduceByKey
				.flatMapToPair(f -> LocalCardinalityAlgorithm.createClassPredicateKey(f));
		flatMapToPair.foreach(t -> System.out.println(t));

		// Group by key and reduce the values to keep only min and max. In the case of
		// the values has only one item, it's the case of max = min.
		finalResult = flatMapToPair.groupByKey()
				.mapToPair(t -> new Tuple2<String, String>(t._1, reduceAndSort(t._2)));
	}

	private static Iterator<Tuple2<String, Long>> createClassPredicateKey(Tuple2<String, Iterable<String>> f) {
		List<Tuple2<String, Long>> mapResults = new ArrayList<>();

		String firstPartKey = f._1;
		Iterator<String> iterator = f._2.iterator();
		while (iterator.hasNext()) {
			String[] split = iterator.next().split(" ");

			mapResults.add(new Tuple2<String, Long>(firstPartKey + "," + split[0], Long.valueOf(split[1])));
		}

		return mapResults.iterator();
	}

	private static Iterable<String> merge(Iterable<String> a, Iterable<String> b) {
		Iterator<String> iteratora = a.iterator();
		Iterator<String> iteratorb = b.iterator();

		if (!iteratora.hasNext() && !iteratorb.hasNext()) {
			return a;
		} else if (!iteratora.hasNext()) {
			return addMinimalPredicates(iteratorb);
		} else if (!iteratorb.hasNext()) {
			return addMinimalPredicates(iteratora);
		} else {
			return Iterables.concat(a, b);
		}
	}

	private static String reduceAndSort(Iterable<Long> values) {
		List<Long> collect = StreamSupport.stream(values.spliterator(), false).sorted().collect(Collectors.toList());

		Long min = collect.get(0);
		Long max = null;
		if (collect.size() == 1) {
			max = min;
		} else {
			max = collect.get(collect.size() - 1);
		}

		return min + "," + max;
	}

	private static Iterable<String> addMinimalPredicates(Iterator<String> a) {
		List<String> arrays = new ArrayList<>();
		while (a.hasNext()) {
			String next = a.next();
			if (!next.isEmpty()) {
				// name 1
				String[] split = next.split(" ");
				arrays.add(next);
				arrays.add(split[0] + " " + 0);
			}
		}

		return arrays;
	}

	private static Tuple2<String, Iterable<String>> extractKeys(Iterable<String> t) {
		Optional<String> findFirst = StreamSupport.stream(t.spliterator(), false)
				.filter(ts -> ts.startsWith(CLASS_DEFINITION_IDENTIFIER)).findFirst();

		Map<String, Long> collect = StreamSupport.stream(t.spliterator(), false)
				.filter(line -> !line.startsWith(CLASS_DEFINITION_IDENTIFIER))
				.collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

		List<String> contents = new ArrayList<>();
		for (Map.Entry<String, Long> entry : collect.entrySet()) {
			contents.add(entry.getKey() + " " + entry.getValue());
		}

		if (findFirst.isPresent()) {
			return new Tuple2<String, Iterable<String>>(findFirst.get(), contents);
		} else {
			return new Tuple2<String, Iterable<String>>(NO_CLASS_DEFINITION, contents);
		}
	}

	private static String simplifyPredicateObject(String predicate, String object) {
		if (TYPE_PREDICATE_NAME.equals(predicate)) {
			return CLASS_DEFINITION_IDENTIFIER + object;
		} else {
			return predicate;
		}
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

	public static class LocalCardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		public LocalCardinalityAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		public LocalCardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}

		private LocalCardinalityAlgorithm build() {
			LocalCardinalityAlgorithm currentInstance = new LocalCardinalityAlgorithm(inputFiles, outputDirectory);
			currentInstance.build();

			return currentInstance;
		}

		public String buildAsText() {
			LocalCardinalityAlgorithm build = this.build();
			return build.saveAsText();
		}

		public void buildAsTextFile() {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			LocalCardinalityAlgorithm build = this.build();
			build.saveAsTextFile();
		}

		public Map<String, Cardinality> buildAsMap() {
			LocalCardinalityAlgorithm build = this.build();
			return build.saveAsMap();
		}
	}
}
