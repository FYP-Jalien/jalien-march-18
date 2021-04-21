package utils.crawler;

import static utils.crawler.CrawlingStatusCode.E_CATALOGUE_MD5_IS_BLANK;
import static utils.crawler.CrawlingStatusCode.E_CATALOGUE_MD5_IS_NULL;
import static utils.crawler.CrawlingStatusCode.E_FILE_EMPTY;
import static utils.crawler.CrawlingStatusCode.E_GUID_NOT_FOUND;
import static utils.crawler.CrawlingStatusCode.E_PFN_DOWNLOAD_FAILED;
import static utils.crawler.CrawlingStatusCode.E_PFN_NOT_READABLE;
import static utils.crawler.CrawlingStatusCode.E_PFN_OFFLINE;
import static utils.crawler.CrawlingStatusCode.E_PFN_XRDSTAT_FAILED;
import static utils.crawler.CrawlingStatusCode.E_UNEXPECTED_ERROR;
import static utils.crawler.CrawlingStatusCode.S_FILE_CHECKSUM_MATCH;
import static utils.crawler.CrawlingStatusCode.S_FILE_CHECKSUM_MISMATCH;
import alien.catalogue.GUID;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.Factory;
import alien.io.protocols.SourceException;
import alien.io.protocols.SourceExceptionCode;
import alien.io.protocols.Xrootd;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.commands.JAliEnCOMMander;
import alien.user.JAKeyStore;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import utils.StatusType;

/**
 * Start the crawling process for a chunk of PFNs
 * Write information on disk for all PFNs analyzed
 * Extract statistics for the entire crawling process
 *
 * @author anegru
 */
public class SEFileCrawler {

	/**
	 * File output format JSON
	 */
	private static final String OUTPUT_FORMAT_JSON = "json";

	/**
	 * File output format CSV
	 */
	private static final String OUTPUT_FORMAT_CSV = "csv";

	/**
	 * The name of the file that holds crawling data
	 */
	private static final String OUTPUT_FILE_NAME = "output";

	/**
	 * The name of the file that holds statistics about crawled files.
	 */
	private static final String STATS_FILE_NAME = "stats";

	/**
	 * The number of command line arguments required
	 */
	private static final int ARGUMENT_COUNT = 5;

	/**
	 * logger
	 */
	private static final Logger logger = ConfigUtils.getLogger(SEFileCrawler.class.getCanonicalName());

	/**
	 * JAliEnCOMMander object
	 */
	private static JAliEnCOMMander commander;

	/**
	 * Xrootd for download operation
	 */
	private static Xrootd xrootd;

	/**
	 * List of crawling results modeled as PFNData. Contains data that is written to output.
	 */
	private static List<PFNData> pfnDataList = new ArrayList<>();

	/**
	 * Storage element object
	 */
	private static SE se;

	/**
	 * Output file format type. (possible values 'json', 'csv')
	 */
	private static String outputFileType;

	/**
	 * Multiple crawling jobs are launched per SE in an interation. The index of the current job.
	 */
	private static int jobIndex;

	/**
	 * The Unix timestamp of the current running iteration
	 */
	private static String iterationTimestamp;

	/**
	 * The count of crawling jobs launched for this SE in the current iteration
	 */
	private static Integer crawlingJobsCount;

	/**
	 * @param args
	 */
	public static void main(final String[] args) {
		ConfigUtils.setApplicationName("SEFileCrawler");
		System.setProperty("jdk.lang.Process.launchMechanism", "VFORK");

		if (!JAKeyStore.loadKeyStore()) {
			logger.log(Level.SEVERE, "No identity found, exiting");
			return;
		}

		commander = JAliEnCOMMander.getInstance();
		xrootd = (Xrootd) Factory.xrootd.clone();

		try {
			parseArguments(args);
			final CrawlingStatistics stats = startCrawler();

			// writing output to disk
			writeJobOutputToDisk(pfnDataList, outputFileType);

			// writing stats to disk
			if (stats != null) {
				final String fileContents = CrawlingStatistics.toJSON(stats).toJSONString();
				CrawlerUtils.uploadToGrid(commander, fileContents, OUTPUT_FILE_NAME, getJobStatsPath());
			}
			else
				logger.log(Level.INFO, "No Stats could be generated in this iteration");
		}
		catch (final Exception exception) {
			logger.log(Level.INFO, exception.getMessage());
			exception.printStackTrace();
		}
	}

	/**
	 * Parse job arguments
	 *
	 * @param args
	 * @throws Exception
	 */
	private static void parseArguments(final String[] args) throws Exception {

		if (args.length != ARGUMENT_COUNT)
			throw new Exception("Number of arguments supplied is incorrect. Expected " + ARGUMENT_COUNT + ", but got " + args.length);

		se = SEUtils.getSE(Integer.parseInt(args[0]));

		if (se == null)
			throw new Exception("Storage element with number " + args[0] + " does not exist");

		iterationTimestamp = args[1];
		jobIndex = Integer.parseInt(args[2]);
		outputFileType = args[3];
		crawlingJobsCount = Integer.valueOf(args[4]);
	}

	/**
	 * Crawl fileCount random files from the SE.
	 *
	 * @return CrawlingStatistics object
	 */
	private static CrawlingStatistics startCrawler() {

		try {
			final Collection<PFN> randomPFNs = getPFNsFromDisk(getSEPath(), "pfn", 3);
			final ArrayList<PFN> pfns = new ArrayList<>(randomPFNs);

			// zero based
			jobIndex = jobIndex - 1;

			final int pfnStartIndex = jobIndex * pfns.size() / crawlingJobsCount.intValue();
			int pfnEndIndex = (jobIndex + 1) * pfns.size() / crawlingJobsCount.intValue();

			if (pfnEndIndex > pfns.size())
				pfnEndIndex = pfns.size();

			logger.info("(Start, End) " + pfnStartIndex + " " + pfnEndIndex);

			final List<PFN> pfnsToCrawl = pfns.subList(pfnStartIndex, pfnEndIndex);
			logger.info("Job will crawl " + pfnsToCrawl.size() + " pfns");

			final long[] timestamps = new long[pfnsToCrawl.size()];

			long totalPFNCount = 0, inaccessiblePFNs = 0, corruptPFNs = 0, okPFNs = 0, unknownStatusPFNs = 0;
			long fileSizeBytes = 0, downloadTotalDurationMillis = 0, downloadedPFNsTotalCount = 0;
			long xrdfsTotalDurationMillis = 0, xrdfsPFNsTotalCount = 0;

			if (pfnsToCrawl.size() == 0)
				return null;

			for (int i = 0; i < pfnsToCrawl.size(); i++) {
				try {
					final PFN currentPFN = pfnsToCrawl.get(i);

					final long startTimestamp = System.currentTimeMillis();
					final PFNData crawlingResult = crawlPFN(currentPFN);
					final long endTimestamp = System.currentTimeMillis();

					logger.info("PFN = " + currentPFN.pfn + " Result =" + crawlingResult);

					timestamps[i] = endTimestamp - startTimestamp;

					if (crawlingResult.getObservedSize() != null)
						fileSizeBytes += crawlingResult.getObservedSize().longValue();

					if (crawlingResult.getDownloadDurationMillis() != null) {
						downloadTotalDurationMillis += crawlingResult.getDownloadDurationMillis().longValue();
						downloadedPFNsTotalCount += 1;
					}

					if (crawlingResult.getXrdfsDurationMillis() != null) {
						xrdfsTotalDurationMillis += crawlingResult.getXrdfsDurationMillis().longValue();
						xrdfsPFNsTotalCount += 1;
					}

					final String statusType = crawlingResult.getStatusType();
					if (statusType.equals(StatusType.FILE_OK.toString()))
						okPFNs += 1;
					else if (statusType.equals(StatusType.FILE_CORRUPT.toString()))
						corruptPFNs += 1;
					else if (statusType.equals(StatusType.FILE_INACCESSIBLE.toString()))
						inaccessiblePFNs += 1;
					else
						unknownStatusPFNs += 1;

					totalPFNCount += 1;
				}
				catch (final Exception e) {
					e.printStackTrace();
					logger.log(Level.SEVERE, "Cannot crawl pfn " + pfns.get(i).pfn + " " + e.getMessage());
				}
			}

			logger.info("Crawling finished for all files");

			final CrawlingStatistics stats = new CrawlingStatistics(
					totalPFNCount,
					okPFNs,
					inaccessiblePFNs,
					corruptPFNs,
					unknownStatusPFNs,
					0L,
					0L,
					fileSizeBytes,
					downloadedPFNsTotalCount,
					downloadTotalDurationMillis,
					xrdfsPFNsTotalCount,
					xrdfsTotalDurationMillis,
					System.currentTimeMillis());

			for (final long timestamp : timestamps)
				stats.crawlingTotalDurationMillis += timestamp;

			stats.crawlingAvgDurationMillis = stats.crawlingTotalDurationMillis / timestamps.length;
			return stats;
		}
		catch (final Exception exception) {
			exception.printStackTrace();
			logger.log(Level.SEVERE, exception.getMessage());
			return null;
		}
	}

	/**
	 * Read the PFNs from the files on disk.
	 *
	 * @param directoryPath
	 * @param fileName
	 * @param retries
	 * @return a list of PFNs to crawl
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static Collection<PFN> getPFNsFromDisk(final String directoryPath, final String fileName, final int retries) throws IOException, ClassNotFoundException {

		final String filePath = directoryPath + fileName;
		final Collection<PFN> pfns = new HashSet<>();

		if (retries == 0)
			return pfns;

		try (
				FileInputStream fileInputStream = new FileInputStream(new File(fileName));
				ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
			final Collection<?> pfnsContainer = (Collection<?>) objectInputStream.readObject();

			for (final Object pfn : pfnsContainer)
				pfns.add((PFN) pfn);

			return pfns;
		}
		catch (final FileNotFoundException exception) {
			try {
				final File downloadedFile = new File(fileName);

				if (downloadedFile.exists() && !downloadedFile.delete())
					logger.log(Level.INFO, "Cannot delete downloaded file " + downloadedFile.getCanonicalPath());

				commander.c_api.downloadFile(filePath, downloadedFile);
				return getPFNsFromDisk(directoryPath, fileName, retries - 1);
			}
			catch (final Exception e) {
				e.printStackTrace();
				// file cannot be downloaded. throw the original exception
				throw exception;
			}
		}
	}

	/**
	 * Crawl the PFN specified in the argument
	 *
	 * @param currentPFN
	 * @return Status of the crawling
	 */
	private static PFNData crawlPFN(final PFN currentPFN) {
		CrawlingStatus status = null;
		PFN pfnToRead = null;
		GUID guid = null;
		Long catalogueFileSize = null, observedFileSize = null;
		String catalogueMD5 = null, observedMD5 = null;
		Long downloadDurationMillis = null, xrdfsDurationMillis = null;

		// fill PFN access token
		try {
			pfnToRead = getPFNWithAccessToken(currentPFN);
		}
		catch (final Exception exception) {
			logger.log(Level.WARNING, exception.getMessage());
			status = new CrawlingStatus(E_PFN_NOT_READABLE, formatError(exception.getMessage()));
		}

		// check if file exists
		if (pfnToRead != null) {
			try {
				guid = commander.c_api.getGUID(pfnToRead.getGuid().guid.toString(), false, false);

				if (guid == null)
					status = new CrawlingStatus(E_GUID_NOT_FOUND, "Cannot get GUID from PFN");
				else
					catalogueFileSize = Long.valueOf(guid.size);
			}
			catch (final Exception exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());
				status = new CrawlingStatus(E_UNEXPECTED_ERROR, formatError(exception.getMessage()));
			}
		}

		// check if file is online
		if (pfnToRead != null && status == null) {
			try {
				final long start = System.currentTimeMillis();
				final String stat = xrootd.xrdstat(pfnToRead, false, false, false);
				final long end = System.currentTimeMillis();
				xrdfsDurationMillis = Long.valueOf(end - start);
				if (stat != null) {
					final int idx = stat.indexOf("Flags");
					if (idx >= 0 && stat.indexOf("Offline", idx) > 0)
						status = new CrawlingStatus(E_PFN_OFFLINE, "PFN is not online");
				}
			}
			catch (final IOException exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());

				if (exception instanceof SourceException) {
					final SourceExceptionCode code = ((SourceException) exception).getCode();
					status = new CrawlingStatus(code, formatError(exception.getMessage()));
				}
				else
					status = new CrawlingStatus(E_PFN_XRDSTAT_FAILED, formatError(exception.getMessage()));
			}
		}

		// check size and checksum
		if (pfnToRead != null && status == null) {
			File downloadedFile = null;

			try {
				final long start = System.currentTimeMillis();
				downloadedFile = xrootd.get(pfnToRead, null);
				final long end = System.currentTimeMillis();
				downloadDurationMillis = Long.valueOf(end - start);
			}
			catch (final IOException exception) {
				exception.printStackTrace();
				logger.log(Level.WARNING, exception.getMessage());

				if (exception instanceof SourceException) {
					final SourceExceptionCode code = ((SourceException) exception).getCode();
					status = new CrawlingStatus(code, formatError(exception.getMessage()));
				}
				else
					status = new CrawlingStatus(E_PFN_DOWNLOAD_FAILED, formatError(exception.getMessage()));
			}

			if (status == null && downloadedFile != null) {
				try {
					observedFileSize = Long.valueOf(downloadedFile.length());
					observedMD5 = IOUtils.getMD5(downloadedFile);
					catalogueMD5 = guid != null ? guid.md5 : null;

					if (downloadedFile.exists() && !downloadedFile.delete())
						logger.log(Level.INFO, "Cannot delete " + downloadedFile.getName());

					if (Long.valueOf(0).equals(catalogueFileSize))
						status = new CrawlingStatus(E_FILE_EMPTY, "Catalogue file size is 0");
					else if (catalogueMD5 == null) {
//						GUIDUtils.updateMd5(guid.guid, observedMD5);
						status = new CrawlingStatus(E_CATALOGUE_MD5_IS_NULL, "Catalogue MD5 is null");
					}
					else if (catalogueMD5.isBlank()) {
//						GUIDUtils.updateMd5(guid.guid, observedMD5);
						status = new CrawlingStatus(E_CATALOGUE_MD5_IS_BLANK, "Catalogue MD5 is blank");
					}
					else if (!catalogueMD5.equalsIgnoreCase(observedMD5))
						status = new CrawlingStatus(S_FILE_CHECKSUM_MISMATCH, "Recomputed checksum does not match catalogue checksum");
					else
						status = new CrawlingStatus(S_FILE_CHECKSUM_MATCH, "Recomputed checksum matches catalogue checksum");
				}
				catch (final IOException exception) {
					exception.printStackTrace();
					logger.log(Level.WARNING, exception.getMessage());
					status = new CrawlingStatus(E_PFN_DOWNLOAD_FAILED, formatError(exception.getMessage()));
				}
			}
		}

		if (status == null)
			status = new CrawlingStatus(E_UNEXPECTED_ERROR, "Unexpected error while processing PFN");

		PFNData pfnData = null;

		if (guid != null) {

			// move to separate function

			pfnData = new PFNData(
					guid.guid.toString(),
					Integer.valueOf(se.seNumber),
					currentPFN.pfn,
					observedFileSize,
					catalogueFileSize,
					observedMD5,
					catalogueMD5,
					downloadDurationMillis,
					xrdfsDurationMillis,
					status.getCode().toString(),
					status.getCode().getType().toString(),
					status.getMessage(),
					Long.valueOf(System.currentTimeMillis()));

			// build the list of pfn data that will be written to disk as crawler output
			pfnDataList.add(pfnData);
		}

		return pfnData;
	}

	/**
	 * Removes newlines and commas from a string so that it can be used in a CSV file
	 *
	 * @param error
	 * @return String
	 */
	private static String formatError(final String error) {
		if (outputFileType.equals(OUTPUT_FORMAT_CSV)) {
			final String errorNew = error.replaceAll("\\R", " ");
			return errorNew.replaceAll(",", " ");
		}

		return error;
	}

	/**
	 * Fill access token of PFN so that it can be read
	 *
	 * @param pfn
	 * @return PFN to read
	 * @throws Exception
	 */
	private static PFN getPFNWithAccessToken(final PFN pfn) throws Exception {
		final GUID guid = pfn.getGuid();

		if (guid == null)
			throw new Exception("PFN " + pfn.pfn + " has a null GUID");

		final List<String> ses = new ArrayList<>();
		final List<String> exses = new ArrayList<>();
		ses.add(se.seName);

		final Collection<PFN> pfnsToRead = commander.c_api.getPFNsToRead(guid, ses, exses);

		if (pfnsToRead == null)
			throw new Exception("Cannot get PFNs to read for " + pfn.pfn);

		final Iterator<PFN> pfnIterator = pfnsToRead.iterator();

		if (!pfnIterator.hasNext())
			throw new Exception("Cannot get PFNs to read for " + pfn.pfn);

		return pfnIterator.next();
	}

	/**
	 * Write the crawling output to disk
	 *
	 * @param fileType
	 */
	private static void writeJobOutputToDisk(final List<PFNData> dataList, final String fileType) {
		try {
			String jobOutput;

			logger.info("Getting job output from data list of size " + dataList.size());
			logger.info("Values " + dataList);

			if (fileType.toLowerCase().equals(OUTPUT_FORMAT_JSON))
				jobOutput = getOutputAsJSON(dataList);
			else
				jobOutput = getOutputAsCSV(dataList);

			logger.info("Got job output with value " + jobOutput);

			CrawlerUtils.uploadToGrid(commander, jobOutput, OUTPUT_FILE_NAME, getJobOutputPath());
		}
		catch (final Exception e) {
			logger.log(Level.WARNING, "Cannot write output to disk " + e.getMessage());
		}
	}

	/**
	 * @param dataList A list of PFN data gathered during the crawling process
	 * @return The job output as JSON
	 */
	@SuppressWarnings("unchecked")
	private static String getOutputAsJSON(final List<PFNData> dataList) {
		final JSONObject result = new JSONObject();
		result.put(se.seName, new JSONArray());
		final JSONArray pfns = (JSONArray) result.get(se.seName);

		for (final PFNData pfnData : dataList) {
			try {
				pfns.add(pfnData.toJSON());
			}
			catch (final Exception e) {
				logger.info("Cannot convert to JSON " + e.getMessage());
				e.printStackTrace();
			}
		}

		return result.toJSONString();
	}

	/**
	 * @param dataList A list of PFN data gathered during the crawling process
	 * @return The crawling output as CSV
	 */
	private static String getOutputAsCSV(final List<PFNData> dataList) {
		final StringBuilder builder = new StringBuilder();
		logger.info("Before for loop");

		for (int i = 0; i < dataList.size(); i++) {
			try {
				final PFNData pfnData = dataList.get(i);

				if (i == 0) {
					logger.info("Adding header");
					builder.append(pfnData.getCsvHeader()).append("\n");
				}

				logger.info("Appending");

				builder.append(pfnData.toCSV()).append("\n");
			}
			catch (final Exception e) {
				logger.info("Cannot convert to CSV " + e.getMessage());
				e.printStackTrace();
			}
		}

		return builder.toString();
	}

	/**
	 * @return The path of the SE in the current iteration
	 */
	private static String getSEPath() {
		return commander.getCurrentDirName() + "iteration_" + iterationTimestamp + "/" + CrawlerUtils.getSEName(se) + "/";
	}

	/**
	 * @return The path of the current crawling job output
	 */
	private static String getJobOutputPath() {
		return getSEPath() + "output/" + OUTPUT_FILE_NAME + "_" + jobIndex + "." + outputFileType;
	}

	/**
	 * @return The path of the current crawling job statistics
	 */
	public static String getJobStatsPath() {
		return getSEPath() + "stats/" + STATS_FILE_NAME + "_" + jobIndex + ".json";
	}
}