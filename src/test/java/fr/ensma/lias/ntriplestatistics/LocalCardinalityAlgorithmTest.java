package fr.ensma.lias.ntriplestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mickael BARON
 */
public class LocalCardinalityAlgorithmTest {

	@Test
	public void buildAsTextTest() {
		// Given
		String inputFiles = "src/test/resources/localcardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder(
				inputFiles).withTypePredicateIdentifier("type");
		String buildAsText = builder.buildAsText();

		// Then
		Assert.assertTrue(buildAsText.contains("AssistantProf,advises,0,1"));
		Assert.assertTrue(buildAsText.contains("Prof,advises,0,2"));
		Assert.assertTrue(buildAsText.contains("Student,name,1,1"));
		Assert.assertTrue(buildAsText.contains("Student,age,0,1"));
		Assert.assertTrue(buildAsText.contains("AssistantProf,age,0,1"));
		Assert.assertTrue(buildAsText.contains("Thing,advises,1,3"));
	}

	@Test
	public void buildAsMapTest() {
		// Given
		String inputFiles = "src/test/resources/localcardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder(
				inputFiles).withTypePredicateIdentifier("type");
		Map<String, Cardinality> buildAsMap = builder.buildAsMap();

		// Then
		Assert.assertEquals(10, buildAsMap.keySet().size());
		Assert.assertEquals(new Long(1), buildAsMap.get("AssistantProf,advises").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("AssistantProf,advises").getMin());
		Assert.assertEquals(new Long(2), buildAsMap.get("Prof,advises").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("Prof,advises").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("Student,name").getMax());
		Assert.assertEquals(new Long(1), buildAsMap.get("Student,name").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("Student,age").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("Student,age").getMin());
		Assert.assertEquals(new Long(3), buildAsMap.get("Thing,advises").getMax());
		Assert.assertEquals(new Long(1), buildAsMap.get("Thing,advises").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("Prof,age").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("Prof,age").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("AssistantProf,age").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("AssistantProf,age").getMin());
	}
	
	//@Test
	public void buildAsFileTest() throws IOException {
		// Given
		String inputFiles = "src/test/resources/localcardinalitiessample.nt";
		long value = System.currentTimeMillis();
		String outputDirectory = "target/" + value;
		
		// When
		ICardinalityAlgorithmBuilder builder = new LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder(
				inputFiles).withOutputDirectory(outputDirectory).withTypePredicateIdentifier("type");
		builder.buildAsTextFile();
		
		// Then
		File file = new File(outputDirectory + "/localcardinalities");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isDirectory());
		
		file = new File(outputDirectory + "/localcardinalities/part-00000");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isFile());
		
		StringBuilder sb = new StringBuilder();
		BufferedReader br = Files.newBufferedReader(Paths.get(outputDirectory + "/localcardinalities/part-00000"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		br.close();

		String fullContent = sb.toString();
		
		Assert.assertTrue(fullContent.contains("AssistantProf,advises,1,1"));
		Assert.assertTrue(fullContent.contains("Prof,advises,1,2"));
		Assert.assertTrue(fullContent.contains("Student,name,1,1"));
		Assert.assertTrue(fullContent.contains("Student,age,0,1"));
	}

	@Test
	public void mergePredicateCardinalitiesCase1Test() {
		// Given
		List<String> left = Arrays.asList("age 1");
		List<String> right = Arrays.asList("age 2", "name 1");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 2"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 0"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 1"));
	}

	@Test
	public void mergePredicateCardinalitiesCase2Test() {
		// Given
		List<String> left = Arrays.asList("age 2", "name 1");
		List<String> right = Arrays.asList("age 1");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 2"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 0"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 1"));
	}

	@Test
	public void mergePredicateCardinalitiesCase3Test() {
		// Given
		List<String> left = Arrays.asList("age 2");
		List<String> right = Arrays.asList("age 1");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 2"));
	}

	@Test
	public void mergePredicateCardinalitiesCase4Test() {
		// Given
		List<String> left = Arrays.asList("age 1", "name 1");
		List<String> right = Arrays.asList("age 2", "name 2");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 2"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 2"));
	}

	@Test
	public void mergePredicateCardinalitiesCase5Test() {
		// Given
		List<String> left = Arrays.asList("name 1", "name 2", "age 1", "age 2");
		List<String> right = Arrays.asList("firstname 2", "name 2");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 0"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 2"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 1"));
		Assert.assertTrue(mergePredicateCardinalities.contains("name 2"));
		Assert.assertTrue(mergePredicateCardinalities.contains("firstname 0"));
		Assert.assertTrue(mergePredicateCardinalities.contains("firstname 2"));
	}

	@Test
	public void mergePredicateCardinalitiesCase6Test() {
		// Given
		List<String> left = Arrays.asList();
		List<String> right = Arrays.asList();

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.isEmpty());
	}

	@Test
	public void mergePredicateCardinalitiesCase7Test() {
		// Given
		List<String> left = Arrays.asList();
		List<String> right = Arrays.asList("age 1");

		// When
		List<String> mergePredicateCardinalities = LocalCardinalityAlgorithm.mergePredicateCardinalities(left, right);

		// Then
		Assert.assertTrue(mergePredicateCardinalities.contains("age 0"));
		Assert.assertTrue(mergePredicateCardinalities.contains("age 1"));
	}
}
