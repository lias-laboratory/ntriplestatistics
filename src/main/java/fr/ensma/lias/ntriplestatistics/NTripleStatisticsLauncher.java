package fr.ensma.lias.ntriplestatistics;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * @author Mickael BARON
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
			"--typeidentifier" }, description = "The type predicate identifier (default: ${DEFAULT-VALUE}). Only used for the Local Cardinality algorithm.", defaultValue = LocalCardinalityAlgorithm.TYPE_PREDICATE_IDENTIFIER_DEFAULT)
	String typePredicateIdentifier;

	enum NTRIPLEStatisticsTypes {
		GLOBAL_CARDINALITIES, LOCAL_CARDINALITIES, CS_CARDINALITIES
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
		}

		builder.withOutputDirectory(output);
				
		long start = System.currentTimeMillis();
		builder.buildAsTextFile();
		System.out.println("Duration: " + ((System.currentTimeMillis() - start) / 1000) + " s");
	}
}
