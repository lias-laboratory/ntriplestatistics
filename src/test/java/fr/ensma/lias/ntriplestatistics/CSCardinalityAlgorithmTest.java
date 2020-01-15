package fr.ensma.lias.ntriplestatistics;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class CSCardinalityAlgorithmTest {
	
	
	@Test
	public void buildAsTextTest() {
		// Given
		String inputFiles = "src/test/resources/cscardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new CSCardinalityAlgorithm.CSCardinalityAlgorithmBuilder(
				inputFiles);
		String buildAsText = builder.buildAsText();
		System.out.println(buildAsText);
		// Then
		Assert.assertTrue(buildAsText.contains("3,type:3,advises:4"));
		Assert.assertTrue(buildAsText.contains("1,type:2,age:1,name:1"));
		Assert.assertTrue(buildAsText.contains("1,type:1,name:1"));
		Assert.assertTrue(buildAsText.contains("1,advises:3"));
		Assert.assertTrue(buildAsText.contains("1,name:1,advises:1"));
	}

	@Test
	public void buildAsCSTest() {
		// Given
		String inputFiles = "src/test/resources/cscardinalitiessample.nt";

		// When
		ICardinalityAlgorithmBuilder builder = new CSCardinalityAlgorithm.CSCardinalityAlgorithmBuilder(
				inputFiles);
		List<CharacteristicSet> buildAsCS = ((CSCardinalityAlgorithm.CSCardinalityAlgorithmBuilder)builder).buildAsCS();

		// Then
		Map<String,Integer> m1=new HashMap<String,Integer>();
		m1.put("type",3);
		m1.put("advises",4);
		CharacteristicSet cs1=new CharacteristicSet(3,m1);
		Map<String,Integer> m2=new HashMap<String,Integer>();
		m2.put("type",2);
		m2.put("age",1);
		m2.put("name",1);
		CharacteristicSet cs2=new CharacteristicSet(1,m2);
		Map<String,Integer> m3=new HashMap<String,Integer>();
		m3.put("type",1);
		m3.put("name",1);
		CharacteristicSet cs3=new CharacteristicSet(1,m3);
		Map<String,Integer> m4=new HashMap<String,Integer>();
		m4.put("advises",3);
		CharacteristicSet cs4=new CharacteristicSet(1,m4);
		Map<String,Integer> m5=new HashMap<String,Integer>();
		m5.put("name",1);
		m5.put("advises",1);
		CharacteristicSet cs5=new CharacteristicSet(1,m5);	
		
		Assert.assertTrue(buildAsCS.contains(cs1));
		Assert.assertTrue(buildAsCS.contains(cs2));
		Assert.assertTrue(buildAsCS.contains(cs3));
		Assert.assertTrue(buildAsCS.contains(cs4));
		Assert.assertTrue(buildAsCS.contains(cs5));	
	}
	
	@Test
	public void buildAsFileTest() throws IOException {
		// Given
		String inputFiles = "src/test/resources/cscardinalitiessample.nt";
		long value = System.currentTimeMillis();
		String outputDirectory = "target/" + value;
		
		// When
		ICardinalityAlgorithmBuilder builder = new CSCardinalityAlgorithm.CSCardinalityAlgorithmBuilder(
				inputFiles).withOutputDirectory(outputDirectory);
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

		Assert.assertTrue(fullContent.contains("3, advises=4, type=3"));
		Assert.assertTrue(fullContent.contains("1, name=1, type=2, age=1"));
		Assert.assertTrue(fullContent.contains("1, name=1, type=1"));
		Assert.assertTrue(fullContent.contains("1, advises=3"));
		Assert.assertTrue(fullContent.contains("1, name=1, advises=1"));
	}
}
