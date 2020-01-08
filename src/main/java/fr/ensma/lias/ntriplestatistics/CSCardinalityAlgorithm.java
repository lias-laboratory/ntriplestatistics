package fr.ensma.lias.ntriplestatistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * @author Louise PARKIN
 */
public class CSCardinalityAlgorithm {
	private String outputDirectory;

	private String inputFiles;

	private JavaSparkContext sc;

	private JavaPairRDD<List<String>,Map<String,Integer>> finalResult;
	
	private static final String SUBJECT_NUMBER = "SUBJECTnb";
	

	private CSCardinalityAlgorithm(String inputFiles, String outputDirectory) {
		this.outputDirectory = outputDirectory;
		this.inputFiles = inputFiles;
		
		SparkConf conf = new SparkConf().setAppName("cs_cardinality").setMaster("local[*]");
		sc = new JavaSparkContext(conf);
	}

	private void build() {
		// Split each line.
		JavaRDD<String[]> rows = sc.textFile(inputFiles).map(line -> line.split(" "));

		// Eliminate object.
		JavaPairRDD<String,String> mapToPair=rows.mapToPair(s -> new Tuple2<String,String>(s[0],s[1]));
		
		// Group by subject.
		JavaPairRDD<String, Iterable<String>> groupBySubject = mapToPair.groupByKey();
		
		// Remove dupplicate predicates and increment count of predicates, then combine sets of predicates, 
		// adding the number of subjects and the cardinalities of predicates
		JavaPairRDD<List<String>,Map<String,Integer>> stop = groupBySubject.mapToPair(f->makeMap(f._2));
		finalResult = stop.reduceByKey((x,y)->reduceSet(x,y));
		
	}
	
	private static Map<String,Integer> reduceSet(Map<String,Integer>a,Map<String,Integer>b){
		for (String key:a.keySet()) {
			a.put(key, a.get(key)+b.get(key));
		}
		return a;
	}
	
	private static Tuple2<List<String>,Map<String,Integer>> makeMap(Iterable<String> predicates){
		Map<String,Integer> predicateMap = new HashMap<String,Integer>();
		List<String> keys= new ArrayList<String>();
		predicateMap.put(SUBJECT_NUMBER,1);
		for (String predicate:predicates) {
			if (keys.contains(predicate)) {
				predicateMap.put(predicate, predicateMap.get(predicate)+1);
			}
			else {
				predicateMap.put(predicate, 1);
				keys.add(predicate);
			}
		}
		return new Tuple2<List<String>,Map<String,Integer>> (keys,predicateMap);
	}
	

	private void saveAsTextFile() {
		finalResult.coalesce(1).saveAsTextFile(outputDirectory + "/cscardinalities");

		sc.close();
	}

	private String saveAsText() {
		StringBuffer newStringBuffer = new StringBuffer();

		List<Tuple2<List<String>,Map<String,Integer>>> collect = finalResult.collect();
		for (Tuple2<List<String>,Map<String,Integer>> tuple2 : collect) {
			newStringBuffer.append(tuple2._2.get(SUBJECT_NUMBER));
			for (String predicate:tuple2._1) {
				newStringBuffer.append(","+predicate+":"+tuple2._2.get(predicate));
			}
			newStringBuffer.append("\n");
		}

		sc.close();
		return newStringBuffer.toString();
	}

	
	//This return type is not compatible with characteristic sets. We replace it with the following method, saving a list of CS
	private Map<String, Cardinality> saveAsMap() {
		Map<String, Cardinality> result = new HashMap<>();

		sc.close();
		return result;
	}
	
	List<CharacteristicSet> saveAsCS() {
		List<CharacteristicSet> result=new ArrayList<CharacteristicSet>();
		List<Tuple2<List<String>,Map<String,Integer>>> collect = finalResult.collect();
		for (Tuple2<List<String>,Map<String,Integer>> tuple2 : collect) {
			Integer subject = tuple2._2.remove(SUBJECT_NUMBER);
			CharacteristicSet newCharacteristicSet = new CharacteristicSet(subject, tuple2._2);
			result.add(newCharacteristicSet);
		}
		return result;
		
	}

	protected static boolean getLineFilter(String line) {
		return !line.startsWith("_");
	}

	public static class CSCardinalityAlgorithmBuilder implements ICardinalityAlgorithmBuilder {
		private String outputDirectory;

		private String inputFiles;

		public CSCardinalityAlgorithmBuilder(String inputFiles) {
			this.inputFiles = inputFiles;
		}

		@Override
		public ICardinalityAlgorithmBuilder withOutputDirectory(String outputDirectory) {
			this.outputDirectory = outputDirectory;

			return this;
		}

		private CSCardinalityAlgorithm build() {
			CSCardinalityAlgorithm currentInstance = new CSCardinalityAlgorithm(inputFiles, outputDirectory);
			currentInstance.build();

			return currentInstance;
		}
		
		public List<CharacteristicSet> buildAsCS() {
			CSCardinalityAlgorithm build = this.build();
			return build.saveAsCS();
		}

		@Override
		public String buildAsText() {
			CSCardinalityAlgorithm build = this.build();
			
			return build.saveAsText();
		}

		@Override
		public void buildAsTextFile() {
			if (this.outputDirectory == null) {
				throw new RuntimeException("OutputDirectory value is missing.");
			}

			CSCardinalityAlgorithm build = this.build();
			build.saveAsTextFile();
		}

		@Override
		public Map<String, Cardinality> buildAsMap() {
			CSCardinalityAlgorithm build = this.build();
			
			return build.saveAsMap();
		}

		@Override
		public ICardinalityAlgorithmBuilder withTypePredicateIdentifier(String typePredicateIdentifier) {
			return null;
		}
	}
}
