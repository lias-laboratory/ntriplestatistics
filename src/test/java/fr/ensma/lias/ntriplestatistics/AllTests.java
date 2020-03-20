package fr.ensma.lias.ntriplestatistics;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import fr.ensma.lias.ntriplestatistics.algorithm.CSCardinalityAlgorithmTest;
import fr.ensma.lias.ntriplestatistics.algorithm.DomainAlgorithmTest;
import fr.ensma.lias.ntriplestatistics.algorithm.GlobalCardinalityAlgorithmTest;
import fr.ensma.lias.ntriplestatistics.algorithm.LocalCardinalityAlgorithmTest;

/**
 * @author Mickael BARON (baron@ensma.fr)
 */
@RunWith(Suite.class)
@SuiteClasses(value = { CSCardinalityAlgorithmTest.class, DomainAlgorithmTest.class, GlobalCardinalityAlgorithmTest.class, LocalCardinalityAlgorithmTest.class,
		})
public class AllTests {

}