package fr.ensma.lias.ntriplestatistics;

import fr.ensma.lias.ntriplestatistics.GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * @author Mickael BARON
 */
@Command(version = "NTripleStatistics 1.0", header = "%nNTripleStatistics: statistic tools for NTriple files.%n", description = "TBA", footer = "Implemented by Mickel BARON (Follow me on Twitter @mickaelbaron).")
public class NTripleStatisticsLauncher implements Runnable {

	@Option(names = { "-h", "--help" }, usageHelp = true, description = "Print usage help and exit.")
	boolean usageHelpRequested;

	@Option(names = { "-i", "--input" }, required = true, description = "The N-Triples files.")
	String input;

	@Option(names = { "-o", "--output" }, required = true, description = "The output directory to save the results.")
	String output;
	
	@Option(names = { "-t", "--type" }, required = true, description = "The type of algorithm to use: ${COMPLETION-CANDIDATES}")
	NTRIPLEStatisticsTypes type;

	enum NTRIPLEStatisticsTypes {
		GLOBAL_CARDINALITIES,
		LOCAL_CARDINALITIES
	}
	
	public static void main(String[] args) {
		new CommandLine(new NTripleStatisticsLauncher()).execute(args);
	}

	@Override
	public void run() {
		GlobalCardinalityAlgorithmBuilder builder = new GlobalCardinalityAlgorithm.GlobalCardinalityAlgorithmBuilder(
				input);
		builder.withOutputDirectory(output);

		builder.buildAsTextFile();
	}
}
