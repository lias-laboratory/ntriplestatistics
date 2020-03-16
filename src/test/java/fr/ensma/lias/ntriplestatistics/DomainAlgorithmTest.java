package fr.ensma.lias.ntriplestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

public class DomainAlgorithmTest {

	@Test
	public void buildAsTextTest() {
		// Given
		String inputFiles = "src/test/resources/domainsample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new DomainAlgorithm.DomainAlgorithmBuilder(
				inputFiles);
		String buildAsText = builder.buildAsText();

		// Then
		Assert.assertTrue(buildAsText.contains("name,Professor"));
		Assert.assertTrue(buildAsText.contains("type,Person"));
		Assert.assertTrue(buildAsText.contains("age,Student,Professor"));
		Assert.assertTrue(buildAsText.contains("advises,FullProfessor"));
	}
	
	@Test
	public void buildAsFileTest() throws IOException {
		// Given
		String inputFiles = "src/test/resources/domainsample.nt";
		long value = System.currentTimeMillis();
		String outputDirectory = "target/" + value;
		
		// When
		ICardinalityAlgorithmBuilder builder = new DomainAlgorithm.DomainAlgorithmBuilder(
				inputFiles).withOutputDirectory(outputDirectory);
		builder.buildAsTextFile();
		
		// Then
		File file = new File(outputDirectory + "/domain");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isDirectory());
		
		file = new File(outputDirectory + "/domain/part-00000");
		Assert.assertTrue(file.exists());
		Assert.assertTrue(file.isFile());
		
		StringBuilder sb = new StringBuilder();
		BufferedReader br = Files.newBufferedReader(Paths.get(outputDirectory + "/domain/part-00000"));
		String line = null;
		while ((line = br.readLine()) != null) {
			sb.append(line);
		}
		br.close();

		String fullContent = sb.toString();

		Assert.assertTrue(fullContent.contains("name,Professor"));
		Assert.assertTrue(fullContent.contains("type,Person"));
		Assert.assertTrue(fullContent.contains("age,Student,Professor"));
		Assert.assertTrue(fullContent.contains("advises,FullProfessor"));
	}
}
