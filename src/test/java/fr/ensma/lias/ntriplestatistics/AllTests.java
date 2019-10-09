package fr.ensma.lias.ntriplestatistics;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * @author Mickael BARON
 */
@RunWith(Suite.class)
@SuiteClasses(value = { GlobalCardinalityAlgorithmTest.class, LocalCardinalityAlgorithmTest.class })
public class AllTests {

}