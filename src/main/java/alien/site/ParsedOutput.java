package alien.site;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import alien.taskQueue.JDL;
import lazyj.commands.SystemCommand;

/**
 * @author costing
 *
 */
public class ParsedOutput {
	private final ArrayList<OutputEntry> jobOutput;
	private final JDL jdl;
	private final long queueId;
	private final String pwd;
	private final String tag;
	private final boolean checkContent;

	/**
	 * @param queueId
	 * @param jdl
	 */
	public ParsedOutput(final long queueId, final JDL jdl) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = "";
		this.tag = "Output";
		this.checkContent = true;
		parseOutput();
	}

	/**
	 * @param queueId
	 * @param jdl
	 * @param path
	 */
	public ParsedOutput(final long queueId, final JDL jdl, final String path) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = path + "/";
		this.tag = "Output";
		this.checkContent = true;
		parseOutput();
	}

	/**
	 * @param queueId
	 * @param jdl
	 * @param path
	 * @param tag
	 */
	public ParsedOutput(final long queueId, final JDL jdl, final String path, final String tag) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = path + "/";
		this.tag = tag;
		this.checkContent = true;
		parseOutput();
	}

	/**
	 * @param queueId
	 * @param jdl
	 * @param path
	 * @param tag
	 * @param checkContent 
	 */
	public ParsedOutput(final long queueId, final JDL jdl, final String path, final String tag, final boolean checkContent) {
		this.jobOutput = new ArrayList<>();
		this.jdl = jdl;
		this.queueId = queueId;
		this.pwd = path + "/";
		this.tag = tag;
		this.checkContent = checkContent;
		parseOutput();
	}

	/**
	 *
	 */
	public void parseOutput() {
		final List<String> files = jdl.getOutputFiles(this.tag);

		System.err.println("Listing getOutputFiles");

		if (files.size() == 0)
			// Create default archive
			files.add("jalien_defarchNOSPEC." + this.queueId + ":stdout,stderr,resources");
		System.err.println(files); // TODELETE

		final Set<String> processedFiles = new HashSet<>();

		for (final String line : files) {
			System.err.println("Line: " + line);

			final String[] parts = line.split("@");

			// System.err.println("Parts: "+parts[0]+" "+parts[1]);

			final String options = parts.length > 1 ? parts[1] : "";

			if (parts[0].contains(":")) {
				// archive
				final String[] archparts = parts[0].split(":");

				System.err.println("Archparts: " + archparts[0] + " " + archparts[1]);

				final ArrayList<String> filesincluded = parsePatternFiles(archparts[1].split(","), processedFiles);

				System.err.println("Adding archive: " + archparts[0] + " and opt: " + options);
				jobOutput.add(new OutputEntry(archparts[0], filesincluded, options, Long.valueOf(queueId), true));
			}
			else {
				// file(s)
				System.err.println("Single file: " + parts[0]);
				final ArrayList<String> filesincluded = parsePatternFiles(parts[0].split(","), processedFiles);
				for (final String f : filesincluded) {
					System.err.println("Adding single: [" + f + "] and opt: [" + options + "]");
					jobOutput.add(new OutputEntry(f, null, options, Long.valueOf(queueId), false));
				}
			}
		}

		System.err.println(jobOutput.toString());

		return;
	}

	private ArrayList<String> parsePatternFiles(final String[] files, final Set<String> alreadySeen) {
		System.err.println("Files to parse patterns: " + Arrays.asList(files).toString());

		final ArrayList<String> filesFound = new ArrayList<>();

		if (!pwd.equals(""))
			for (final String file : files) {
				System.err.println("Going to parse: " + file);
				if (file.isBlank())
					System.err.println("Looks like we have an empty file. Ignoring: " + file);
				else if (file.contains("*")){
					final String[] parts = SystemCommand.bash("find " + file + " ! -type d").stdout.split("\n");
					if (parts.length > 0)
					for (String f : parts) {
						f = f.trim();
						if (f.length() > 0) {
							if (!alreadySeen.contains(f)) {
								System.err.println("Adding file from ls: " + f);
								filesFound.add(f);
								alreadySeen.add(f);
							}
							else
								System.err.println("Ignoring duplicate file: " + f);
						}
					}
				}
				else {
					if (!alreadySeen.contains(file)) {
						filesFound.add(file);
						alreadySeen.add(file);
					}
					else
						System.err.println("Ignoring duplicate file: " + file);

					if (checkContent) {
						File fsFile = new File(file);
						if (!fsFile.exists())
							throw new NullPointerException("File " + file + " for archive " + "?" + " doesn't exist or cannot be read!");
					}

				}
			}

		System.err.println("Returned parsed array: " + filesFound.toString());

		return filesFound;
	}

	/**
	 * @return list of entries
	 */
	public ArrayList<OutputEntry> getEntries() {
		return this.jobOutput;
	}

}
