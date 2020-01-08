package fr.ensma.lias.ntriplestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mickael BARON
 */
public class GlobalCardinalityAlgorithmTest {

	@Test
	public void buildAsMapTest() {
		// Given
		String inputFiles = "src/test/resources/globalcardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(
				inputFiles);
		Map<String, Cardinality> buildAsMap = builder.buildAsMap();

		// Then
		Assert.assertEquals(4, buildAsMap.keySet().size());
		Assert.assertEquals(new Long(2), buildAsMap.get("is_attending").getMax());
		Assert.assertEquals(new Long(1), buildAsMap.get("is_attending").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("is").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("is_eating").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is_eating").getMin());
		Assert.assertEquals(new Long(2), buildAsMap.get("is_sleeping").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is_sleeping").getMin());
	}

	@Test
	public void buildAsTextTest() {
		// Given
		String inputFiles = "src/test/resources/globalcardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(
				inputFiles);
		String buildAsText = builder.buildAsText();

		// Then
		Assert.assertTrue(buildAsText.contains("is_sleeping,0,2"));
		Assert.assertTrue(buildAsText.contains("is,0,1"));
		Assert.assertTrue(buildAsText.contains("is_attending,1,2"));
		Assert.assertTrue(buildAsText.contains("is_eating,0,1"));
	}
	
	//@Test
	public void buildAsFileTest() throws IOException {
		// Given
		String inputFiles = "src/test/resources/globalcardinalitiessample.nt";
		long value = System.currentTimeMillis();
		String outputDirectory = "target/" + value;
		
		// When
		ICardinalityAlgorithmBuilder builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(
				inputFiles).withOutputDirectory(outputDirectory);
		builder.buildAsTextFile();
		
		// Then
		File file = new File(outputDirectory + "/globalcardinalities");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isDirectory());
		
		file = new File(outputDirectory + "/globalcardinalities/part-00000");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isFile());
		
		StringBuilder sb = new StringBuilder();
		BufferedReader br = Files.newBufferedReader(Paths.get(outputDirectory + "/globalcardinalities/part-00000"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		br.close();

		String fullContent = sb.toString();
		
		Assert.assertTrue(fullContent.contains("is_sleeping,0,2"));
		Assert.assertTrue(fullContent.contains("is,0,1"));
		Assert.assertTrue(fullContent.contains("is_attending,1,2"));
		Assert.assertTrue(fullContent.contains("is_eating,0,1"));
	}
}
