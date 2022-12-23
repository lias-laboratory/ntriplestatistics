package fr.ensma.lias.ntriplestatistics.algorithm;

import java.util.ArrayList;
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
public class CSAlgorithm {

	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;
	
	private JavaPairRDD<String, String> result;

	private static String separatorIdentifier;
	
	private CSAlgorithm(String inputFiles, String outputDirectory, String separator) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;
		CSAlgorithm.separatorIdentifier = separator;

		SparkConf conf = new SparkConf().setAppName("cs_cardinality").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		// Split each line.
		JavaRDD<String[]> rows = sc.textFile(inputFiles).map(line -> line.split(separatorIdentifier));
		
		// Eliminate object.
		JavaPairRDD<String, String> mapToPair = rows.mapToPair(s -> new Tuple2<String, String>(s[0], s[1]));

		// Group by subject.
		JavaPairRDD<String, Iterable<String>> groupBySubject = mapToPair.groupByKey();
		
		//Combine identical predicates
		JavaPairRDD<Iterable<String>,Map<String, Integer>> listPredicates = groupBySubject.mapToPair(s -> combinePredicates(s._2));
		
		//Get maximum cardinalities
		JavaPairRDD<Iterable<String>,Map<String, Integer>> groupPredicates = listPredicates.reduceByKey((x,y) -> maxCard(x,y));
		
		//Return lists of predicates and lists of cardinalities
		result = groupPredicates.mapToPair(s -> makeString(s._2));

	}
	
	private static Tuple2<String,String> makeString(Map<String, Integer> predicates) {
		String lStr="";
		String lInt="";
		for (String predicate : predicates.keySet()) {
			if (lStr.equals("")) {
				lStr=predicate;
				lInt=predicates.get(predicate).toString();
			}
			else {
				lStr+=";"+predicate;
				lInt+=";"+predicates.get(predicate);
			}
		}
		return new Tuple2<String, String>(lStr,lInt);
	}
	
	private static Map<String, Integer> maxCard(Map<String, Integer> a, Map<String, Integer> b) {
		Map<String, Integer> res = new HashMap<String, Integer>();
		for (String key : a.keySet()) {
			if (a.get(key)>b.get(key))
				res.put(key,a.get(key));
			else 
				res.put(key,b.get(key));
		}
		return res;
	}
	
	private static Tuple2<Iterable<String>,Map<String, Integer>> combinePredicates(Iterable<String> predicates) {
		Map<String, Integer> predicateMap = new HashMap<String, Integer>();
		List<String> keys=new ArrayList<String>();
		for (String predicate : predicates) {
			if (predicateMap.containsKey(predicate)){
				predicateMap.put(predicate, predicateMap.get(predicate) + 1);
			} else {
				predicateMap.put(predicate, 1);
				keys.add(predicate);
			}
		}
		return new Tuple2<Iterable<String>,Map<String, Integer>>(keys, predicateMap);
	}

	private void saveAsTextFile() {
		result.coalesce(1).saveAsTextFile(outputDirectory + "/cscardinalities");

		sc.close();
	}

	private String saveAsText() {
		StringBuffer newStringBuffer = new StringBuffer();

		List<Tuple2<String, String>> collect = result.collect();
		for (Tuple2<String, String> tuple2 : collect) {
			newStringBuffer.append(tuple2._1+","+tuple2._2+"\n");
		}

		sc.close();
		return newStringBuffer.toString();
	}

	// This return type is not compatible with characteristic sets. We replace it
	// with the following method, saving a list of CS
	private Map<String, Cardinality> saveAsMap() {
		Map<String, Cardinality> result = new HashMap<>();

		sc.close();
		return result;
	}

	protected static boolean getLineFilter(String line) {
		return !line.startsWith("_");
	}

	public static class CSCardinalityAlgorithmBuilder implements ICardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		private String separatorIdentifier = ICardinalityAlgorithmBuilder.DEFAULT_SEPARATOR;
		
		public CSCardinalityAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		@Override
		public ICardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}

		private CSAlgorithm build() {
			CSAlgorithm currentInstance = new CSAlgorithm(inputFiles, outputDirectory, separatorIdentifier);
			currentInstance.build();

			return currentInstance;
		}


		@Override
		public String buildAsText() {
			CSAlgorithm build = this.build();

			return build.saveAsText();
		}

		@Override
		public void buildAsTextFile() {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			CSAlgorithm build = this.build();
			build.saveAsTextFile();
		}

		public Map<String, Cardinality> buildAsMap() {
			CSAlgorithm build = this.build();

			return build.saveAsMap();
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
