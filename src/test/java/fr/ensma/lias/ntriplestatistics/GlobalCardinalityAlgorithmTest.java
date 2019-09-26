package fr.ensma.lias.ntriplestatistics;

import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import fr.ensma.lias.ntriplestatistics.GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder;

/**
 * @author Mickael BARON
 */
public class GlobalCardinalityAlgorithmTest {

	@Test
	public void buildAsMapTest() {
		// Given
		String inputFiles = "src/test/resources/sample.nt";
		
		
		// When
		GlobalCardinalityAlgorithmBuilder builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(inputFiles);
		Map<String, Cardinality> buildAsMap = builder.buildAsMap();
				
		// Then
		Assert.assertEquals(4, buildAsMap.keySet().size());
		Assert.assertEquals(new Long(2), buildAsMap.get("is_attending").getMax());
		Assert.assertEquals(new Long(1), buildAsMap.get("is_attending").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("is").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("is_eating").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is_eating").getMin());
		Assert.assertEquals(new Long(1), buildAsMap.get("is_sleeping").getMax());
		Assert.assertEquals(new Long(0), buildAsMap.get("is_sleeping").getMin());
	}
}
