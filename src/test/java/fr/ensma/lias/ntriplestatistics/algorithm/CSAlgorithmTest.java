package fr.ensma.lias.ntriplestatistics.algorithm;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

/**
 * @author Mickael BARON (baron@ensma.fr)
 */
public class CSAlgorithmTest {

	@Test
	public void buildAsFileTest() throws IOException {
		// Given
		String inputFiles = "src/test/resources/cscardinalitiessample.nt";
		long value = System.currentTimeMillis();
		String outputDirectory = "target/" + value;

		// When
		ICardinalityAlgorithmBuilder builder = new CSAlgorithm.CSCardinalityAlgorithmBuilder(inputFiles)
				.withOutputDirectory(outputDirectory);
		builder.buildAsTextFile();

		// Then
		File file = new File(outputDirectory + "/cscardinalities");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isDirectory());

		file = new File(outputDirectory + "/cscardinalities/part-00000");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isFile());

		StringBuilder sb = new StringBuilder();
		BufferedReader br = Files.newBufferedReader(Paths.get(outputDirectory + "/cscardinalities/part-00000"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		br.close();

		String fullContent = sb.toString();
		Assert.assertTrue(fullContent.contains("name;type,1;1"));
		Assert.assertTrue(fullContent.contains("advises;type,2;1"));
		Assert.assertTrue(fullContent.contains("advises,3"));
		Assert.assertTrue(fullContent.contains("name;advises,1;1"));
		Assert.assertTrue(fullContent.contains("name;type;age,1;2;1"));
	}
	
		
	@Test
	public void buildAsTextTest() throws IOException {

		// Given
		String inputFiles = "src/test/resources/cscardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new CSAlgorithm.CSCardinalityAlgorithmBuilder(inputFiles);
		String buildAsText = builder.buildAsText();

		// Then
		
		Assert.assertTrue(buildAsText.contains("name;type,1;1"));
		Assert.assertTrue(buildAsText.contains("advises;type,2;1"));
		Assert.assertTrue(buildAsText.contains("advises,3"));
		Assert.assertTrue(buildAsText.contains("name;advises,1;1"));
		Assert.assertTrue(buildAsText.contains("name;type;age,1;2;1"));
	}
	
	@Test
	public void buildAsTextFromTabContentTest() {
		// Given
		String inputFiles = "src/test/resources/cscardinalitiessamplewithtab.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new CSAlgorithm.CSCardinalityAlgorithmBuilder(inputFiles);
		String buildAsText = builder.withSeparator("\t").buildAsText();

		// Then
		Assert.assertTrue(buildAsText.contains("name;type,1;1"));
		Assert.assertTrue(buildAsText.contains("advises;type,2;1"));
		Assert.assertTrue(buildAsText.contains("advises,3"));
		Assert.assertTrue(buildAsText.contains("name;advises,1;1"));
		Assert.assertTrue(buildAsText.contains("name;type;age,1;2;1"));
	}
	
}
