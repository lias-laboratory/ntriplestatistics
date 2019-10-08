package fr.ensma.lias.ntriplestatistics;

import org.junit.Test;

import fr.ensma.lias.ntriplestatistics.LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder;

/**
 * @author Mickael BARON
 */
public class LocalCardinalityAlgorithmTest {

	@Test
	public void buildAsMapTest() {
		// Given
		String inputFiles = "src/test/resources/localcardinalitiessample.nt";
		
		
		// When
		LocalCardinalityAlgorithmBuilder builder = new LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder(inputFiles);
		System.out.println(builder.buildAsText());
		// Then
	}
}
