package fr.ensma.lias.ntriplestatistics;

import fr.ensma.lias.ntriplestatistics.algorithm.CSAlgorithm;
import fr.ensma.lias.ntriplestatistics.algorithm.CSCardinalityAlgorithm;
import fr.ensma.lias.ntriplestatistics.algorithm.DomainAlgorithm;
import fr.ensma.lias.ntriplestatistics.algorithm.GlobalCardinalityAlgorithm;
import fr.ensma.lias.ntriplestatistics.algorithm.ICardinalityAlgorithmBuilder;
import fr.ensma.lias.ntriplestatistics.algorithm.LocalCardinalityAlgorithm;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * @author Mickael BARON (baron@ensma.fr)
 */
@Command(version = "NTripleStatistics 1.0", header = "%nNTripleStatistics: statistic tools for NTriple files.%n", description = "TBA", footer = "Coded with ♥ by Mickael BARON (Follow me on Twitter @mickaelbaron).")
public class NTripleStatisticsLauncher implements Runnable {

	@Option(names = { "-h", "--help" }, usageHelp = true, description = "Print usage help and exit.")
	boolean usageHelpRequested;

	@Option(names = { "-i",
			"--input" }, required = true, description = "The N-Triples files (example: *.nt or 2015-11-02-Abutters.way.sorted.nt).")
	String input;

	@Option(names = { "-o", "--output" }, required = true, description = "The output directory to save the results.")
	String output;

	@Option(names = { "-a",
			"--algorithm" }, required = true, description = "The algorithm to use: ${COMPLETION-CANDIDATES}.")
	NTRIPLEStatisticsTypes type;

	@Option(names = { "-t",
			"--typeidentifier" }, description = "The type predicate identifier (default: ${DEFAULT-VALUE}). Only used for the Local Cardinality algorithm.", defaultValue = ICardinalityAlgorithmBuilder.TYPE_PREDICATE_IDENTIFIER_DEFAULT)
	String typePredicateIdentifier;
	
	@Option(names = { "-s",
			"--separator" }, description = "The separator expression between the subject, predicate and object contents.", defaultValue = ICardinalityAlgorithmBuilder.DEFAULT_SEPARATOR)
	String separator;

	enum NTRIPLEStatisticsTypes {
		GLOBAL_CARDINALITIES, LOCAL_CARDINALITIES, CS_CARDINALITIES, DOMAIN, CS
	}

	public static void main(String[] args) {
		new CommandLine(new NTripleStatisticsLauncher()).execute(args);
	}

	@Override
	public void run() {
		ICardinalityAlgorithmBuilder builder = null;

		switch (type) {
			case GLOBAL_CARDINALITIES: {
				builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(input);
				break;
			}
			case LOCAL_CARDINALITIES: {
				builder = new LocalCardinalityAlgorithm.LocalCardinalityAlgorithmBuilder(input);
				builder.withTypePredicateIdentifier(typePredicateIdentifier);
				break;
			}
			case CS_CARDINALITIES: {
				builder = new CSCardinalityAlgorithm.CSCardinalityAlgorithmBuilder(input);
				break;
			}
			case CS: {
				builder = new CSAlgorithm.CSCardinalityAlgorithmBuilder(input);
				break;
			}
			case DOMAIN: {
				builder = new DomainAlgorithm.DomainAlgorithmBuilder(input);
				break;
			}
		}

		builder.withSeparator(separator).withOutputDirectory(output);
				
		long start = System.currentTimeMillis();
		builder.buildAsTextFile();
		System.out.println("Duration: " + ((System.currentTimeMillis() - start) / 1000) + " s");
	}
}
