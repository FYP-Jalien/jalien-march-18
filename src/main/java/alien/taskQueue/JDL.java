package alien.taskQueue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.XmlCollection;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import lazyj.Format;
import lazyj.StringFactory;
import lazyj.Utils;

/**
 * @author costing
 *
 */
public class JDL implements Serializable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(JDL.class.getCanonicalName());

	/**
	 *
	 */
	private static final long serialVersionUID = -4803377858842338873L;
	private final Map<String, Object> jdlContent = new LinkedHashMap<>();

	/**
	 * Empty constructor. The values can be populated with {@link #set(String, Object)} and {@link #append(String, String...)}
	 */
	public JDL() {
		// empty
	}

	/**
	 * A file in the catalogue
	 *
	 * @param file
	 * @throws IOException
	 */
	public JDL(final LFN file) throws IOException {
		this(IOUtils.getContents(file));
	}

	/**
	 * A file in the catalogue
	 *
	 * @param file
	 * @throws IOException
	 */
	public JDL(final GUID file) throws IOException {
		this(IOUtils.getContents(file));
	}

	/**
	 * a local file
	 *
	 * @param file
	 * @throws IOException
	 */
	public JDL(final File file) throws IOException {
		this(Utils.readFile(file.getAbsolutePath()));
	}

	/**
	 * a job ID
	 *
	 * @param jobID
	 * @throws IOException
	 */
	public JDL(final long jobID) throws IOException {
		this(Job.sanitizeJDL(TaskQueueUtils.getJDL(jobID)));
	}

	/**
	 * a job ID
	 *
	 * @param jobID
	 * @param originalJDL
	 *            whether to load the original JDL (submitted by the user) or the processed one (if available)
	 * @throws IOException
	 */
	public JDL(final long jobID, final boolean originalJDL) throws IOException {
		this(Job.sanitizeJDL(TaskQueueUtils.getJDL(jobID, originalJDL)));
	}

	/**
	 * @param jdl
	 * @return jdl content stripped of comments
	 */
	public static final String removeComments(final String jdl) {
		if (jdl == null || jdl.length() == 0 || jdl.indexOf('#') < 0)
			return jdl;

		final BufferedReader br = new BufferedReader(new StringReader(jdl));

		String line;

		final StringBuilder sb = new StringBuilder(jdl.length());

		try {
			while ((line = br.readLine()) != null) {
				if (line.length() == 0 || line.trim().startsWith("#"))
					continue;

				sb.append(line).append('\n');
			}
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// cannot be
		}

		return sb.toString();
	}

	/**
	 * the full contents
	 *
	 * @param origContent
	 * @throws IOException
	 */
	public JDL(final String origContent) throws IOException {
		if (origContent == null || origContent.length() == 0)
			throw new IOException("Content is " + (origContent == null ? "null" : "empty"));

		int iPrevPos = 0;

		int idxEqual = -1;

		final String content = removeComments(origContent);

		while ((idxEqual = content.indexOf('=', iPrevPos + 1)) > 0) {
			final String sKey = clean(content.substring(iPrevPos, idxEqual).trim());

			checkKeySyntax(sKey);

			int idxEnd = idxEqual + 1;

			boolean bEsc = false;
			boolean bQuote = false;
			boolean bClean = false;

			outer:
			while (idxEnd < content.length()) {
				final char c = content.charAt(idxEnd);

				switch (c) {
					case '\\':
						bEsc = !bEsc;

						break;
					case '"':
						if (!bEsc)
							bQuote = !bQuote;

						bEsc = false;

						break;
					case ';':
						if (!bEsc && !bQuote) {
							bClean = true;
							break outer;
						}

						bEsc = false;

						break;
					default:
						bEsc = false;
				}

				idxEnd++;
			}

			if (bEsc || bQuote)
				throw new IOException("JDL syntax error: unfinished " + (bQuote ? "quotes" : "escape") + " in the value of tag " + sKey);

			if (!bClean)
				// throw new
				// IOException("JDL syntax error: Tag "+sKey+" doesn't finish with a semicolumn");
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "JDL syntax error: Tag " + sKey + " doesn't finish with a semicolumn, full text is\n" + content);

			final String sValue = content.substring(idxEqual + 1, idxEnd).trim();

			final Object value = parseValue(sValue, sKey);

			// System.err.println(sKey +" = "+value);

			if (value != null)
				jdlContent.put(sKey, value);

			iPrevPos = idxEnd + 1;
		}
	}

	private static void checkKeySyntax(final String sKey) throws IOException {
		if (sKey == null)
			throw new IOException("Key cannot be null");

		if (sKey.length() == 0)
			throw new IOException("Key cannot be the empty string");

		for (final char c : sKey.toCharArray())
			if (!Character.isLetterOrDigit(c) && c != '_')
				throw new IOException("Illegal character '" + c + "' in key '" + sKey + "'");
	}

	private static String clean(final String input) {
		String output = input;

		while (output.startsWith("#")) {
			final int idx = output.indexOf('\n');

			if (idx < 0)
				return StringFactory.get("");

			output = output.substring(idx + 1);
		}

		while (output.startsWith("\n"))
			output = output.substring(1);

		while (output.endsWith("\n"))
			output = output.substring(0, output.length() - 1);

		return StringFactory.get(output);
	}

	/**
	 * @return the set of keys present in the JDL
	 */
	public Set<String> keySet() {
		return Collections.unmodifiableSet(jdlContent.keySet());
	}

	/**
	 * Get the value of a key
	 *
	 * @param key
	 *
	 * @return the value, can be a String, a Number, a Collection ...
	 */
	public Object get(final String key) {
		for (final Map.Entry<String, Object> entry : jdlContent.entrySet())
			if (entry.getKey().equalsIgnoreCase(key))
				return entry.getValue();

		return null;
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return the boolean for this key
	 */
	public boolean getb(final String key, final boolean defaultValue) {
		return Utils.stringToBool(gets(key), defaultValue);
	}

	/**
	 * Get the value of this key as String
	 *
	 * @param key
	 *
	 * @return the single value if this was a String, the first entry of a Collection (based on the iterator)...
	 */
	public String gets(final String key) {
		final Object o = get(key);

		return getString(o);
	}

	/**
	 * @param key
	 * @return the integer value, or <code>null</code> if the key is not defined or is not a number
	 */
	public Integer getInteger(final String key) {
		final Object o = get(key);

		if (o == null)
			return null;

		if (o instanceof Integer)
			return (Integer) o;

		if (o instanceof Number)
			return Integer.valueOf(((Number) o).intValue());

		try {
			return Integer.valueOf(getString(o));
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			// ignore
		}

		return null; // not an integer
	}

	/**
	 * @param key
	 * @return the long value, or <code>null</code> if the key is not defined or is not a Number or a valid representation of one
	 */
	public Long getLong(final String key) {
		final Object o = get(key);

		if (o == null)
			return null;

		if (o instanceof Long)
			return (Long) o;

		if (o instanceof Number)
			return Long.valueOf(((Number) o).longValue());

		if (o instanceof CharSequence) {
			try {
				return Long.valueOf(((CharSequence) o).toString());
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}
		}
		else
			try {
				return Long.valueOf(getString(o));
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}

		return null; // not an integer
	}

	/**
	 * @param key
	 * @return the float value, or <code>null</code> if the key is not defined or is not a number
	 */
	public Float getFloat(final String key) {
		final Object o = get(key);

		if (o == null)
			return null;

		if (o instanceof Float)
			return (Float) o;

		if (o instanceof Number)
			return Float.valueOf(((Number) o).floatValue());

		try {
			return Float.valueOf(getString(o));
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			// ignore
		}

		return null; // not an integer
	}

	private static String getString(final Object o) {
		if (o == null)
			return null;

		if (o instanceof Collection<?>) {
			final Collection<?> c = (Collection<?>) o;

			if (c.size() > 0)
				return getString(c.iterator().next());
		}

		return o.toString();
	}

	private static final Object parseValue(final String value, final String tag) throws IOException {
		if (value.startsWith("\"")) {
			if (!value.endsWith("\""))
				throw new IOException("JDL syntax error: quotes do not close at the end of string for tag " + tag);

			return StringFactory.get(value.substring(1, value.length() - 1));
		}

		if (value.startsWith("{")) {
			if (!value.endsWith("}"))
				throw new IOException("JDL syntax error: unclosed brackets in the value of tag " + tag);

			return toSet(value.substring(1, value.length() - 1));
		}

		boolean possibleInt = true;
		boolean possibleDouble = true;

		for (char c : value.toCharArray()) {
			if (c >= '0' && c <= '9')
				continue;

			possibleInt = false;

			if (c != '.' && c != '+' && c != '-' && c != 'E' && c != 'e')
				possibleDouble = false;

			if (!possibleInt && !possibleDouble)
				break;
		}

		if (possibleInt) {
			if (value.length() <= 10)
				try {
					return Integer.valueOf(value);
				}
				catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
					// ignore
				}

			try {
				return Long.valueOf(value);
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}
		}

		if (possibleDouble)
			try {
				return Double.valueOf(value);
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}

		// signal that this is not a string in quotes
		return new StringBuilder(value);
	}

	private static final Pattern PANDA_RUN_NO = Pattern.compile(".*/run(\\d+)$");

	/**
	 * Get the run number if this job is a simulation job
	 *
	 * @return run number
	 */
	public int getSimRun() {
		final String split = gets("splitarguments");

		if (split == null) {
			// is it a Panda production ?

			final String sOutputDir = getOutputDir();

			if (sOutputDir == null || sOutputDir.length() == 0)
				return -1;

			final Matcher m = PANDA_RUN_NO.matcher(sOutputDir);

			if (m.matches())
				return Integer.parseInt(m.group(1));

			return -1;
		}

		if (split.indexOf("sim") < 0)
			return -1;

		final StringTokenizer st = new StringTokenizer(split);

		while (st.hasMoreTokens()) {
			final String s = st.nextToken();

			if (s.equals("--run")) {
				if (st.hasMoreTokens()) {
					final String run = st.nextToken();

					try {
						return Integer.parseInt(run);
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						return -1;
					}
				}

				return -1;
			}
		}

		return -1;
	}

	/**
	 * Get the number of jobs this masterjob will split into. Only works for productions that split in a fixed number of jobs.
	 *
	 * @return the number of subjobs
	 */
	public int getSplitCount() {
		final String split = gets("split");

		if (split == null || split.length() == 0)
			return -1;

		if (split.startsWith("production:"))
			try {
				return Integer.parseInt(split.substring(split.lastIndexOf('-') + 1));
			}
			catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
				// ignore
			}

		return -1;
	}

	/**
	 * @return the InputFile tag, as LFNs. If the InputFile is an XML collection, return the entire content of that collection.
	 */
	public Collection<LFN> getInputLFNs() {
		final List<String> dataFiles = getInputData();

		final List<LFN> ret = new LinkedList<>();

		final List<String> otherInputFiles = new LinkedList<>();

		if (dataFiles != null) {
			for (final String file : dataFiles) {
				if (file.endsWith(".xml"))
					try {
						final XmlCollection x = new XmlCollection(LFNUtils.getLFN(file));

						return x;
					}
					catch (@SuppressWarnings("unused") final IOException ioe) {
						// ignore
					}

				otherInputFiles.add(file);
			}
		}

		final List<LFN> tempList = LFNUtils.getLFNs(true, otherInputFiles);

		if (tempList != null && tempList.size() > 0)
			ret.addAll(tempList);

		return ret;
	}

	/**
	 * Get the list of input files
	 *
	 * @return the list of input files
	 */
	public List<String> getInputFiles() {
		return getInputFiles(true);
	}

	/**
	 * @return the input data
	 */
	public List<String> getInputData() {
		return getInputData(true);
	}

	/**
	 * Get the list of input data
	 *
	 * @param bNodownloadIncluded
	 *            include or not the files with the ",nodownload" option
	 *
	 * @return list of input data to the job
	 */
	public List<String> getInputData(final boolean bNodownloadIncluded) {
		final List<String> inputData = getInputList(bNodownloadIncluded, "InputData");

		if (inputData != null)
			return inputData;

		return getInputList(bNodownloadIncluded, "InputDataCollection");
	}

	/**
	 * Get the list of input files
	 *
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is indicated in the JDL
	 * @return list of input files
	 */
	public List<String> getInputFiles(final boolean bNodownloadIncluded) {
		List<String> ret = getInputList(bNodownloadIncluded, "InputFile");

		if (ret == null)
			ret = getInputList(bNodownloadIncluded, "InputBox");

		return ret;
	}

	/**
	 * Get the list of output files
	 *
	 * @return list of output files
	 */
	public List<String> getOutputFiles() {
		List<String> ret = getInputList(false, "Output");
		if (ret == null)
			ret = new LinkedList<>();
		final List<String> retf = getInputList(false, "OutputFile");
		if (retf != null)
			ret.addAll(retf);
		final List<String> reta = getInputList(false, "OutputArchive");
		if (reta != null)
			ret.addAll(reta);

		return ret;
	}

	/**
	 * Get the list of output files
	 *
	 * @param tag
	 *            JDL tag to take the list from
	 *
	 * @return list of output files
	 */
	public List<String> getOutputFiles(final String tag) {
		List<String> ret = getInputList(false, tag);
		if (ret == null)
			ret = new LinkedList<>();

		return ret;
	}

	/**
	 * Get the list of arguments
	 *
	 * @return list of arguments
	 */
	public List<String> getArguments() {
		return getInputList(false, "Arguments");
	}

	/**
	 * Get the user name of the job
	 *
	 * @return user
	 */
	public String getUser() {
		return gets("User");
	}

	/**
	 * Get the executable
	 *
	 * @return executable
	 */
	public String getExecutable() {
		return gets("Executable");
	}

	/**
	 * Get the output directory, the unparsed value of the "OutputDir" tag.
	 *
	 * @return output directory
	 * @see #getOutputDir()
	 */
	public String getOutputDirectory() {
		return gets("OutputDir");
	}

	/**
	 * Get the list of input files for a given tag
	 *
	 * @param bNodownloadIncluded
	 *            flag to include/exclude the files for which ",nodownload" is indicated in the JDL
	 * @param sTag
	 *            tag to extract the list from
	 * @return input list
	 */
	public List<String> getInputList(final boolean bNodownloadIncluded, final String sTag) {
		final Object o = get(sTag);

		if (o == null)
			return null;

		final List<String> ret = new LinkedList<>();

		if (o instanceof CharSequence) {
			final String s = ((CharSequence) o).toString();

			if (bNodownloadIncluded || s.indexOf(",nodownload") < 0) {
				final String temp = removeLF(s);

				if (temp.length() > 0)
					ret.add(temp);
			}

			return ret;
		}

		if (o instanceof Collection<?>)
			for (final Object o2 : (Collection<?>) o)
				if (o2 instanceof String) {
					final String s = (String) o2;

					if (bNodownloadIncluded || s.indexOf(",nodownload") < 0) {
						final String temp = removeLF(s);

						if (temp.length() > 0)
							ret.add(temp);
					}
				}

		return ret;
	}

	private static String removeLF(final String s) {
		String ret = s;

		if (ret.startsWith("LF:"))
			ret = ret.substring(3);

		final int idx = ret.indexOf(",nodownload");

		if (idx >= 0)
			ret = ret.substring(0, idx);

		return ret;
	}

	private static Set<String> toSet(final String value) {
		final Set<String> ret = new LinkedHashSet<>();

		int idx = value.indexOf('"');

		if (idx < 0) {
			if (!value.isBlank())
				ret.add(value);
			
			return ret;
		}

		do {
			int idx2 = value.indexOf('"', idx + 1);

			if (idx2 < 0)
				return ret;

			while (value.charAt(idx2 - 1) == '\'') {
				idx2 = value.indexOf('"', idx2 + 1);

				if (idx2 < 0)
					return ret;
			}

			ret.add(StringFactory.get(value.substring(idx + 1, idx2)));

			idx = value.indexOf('"', idx2 + 1);
		} while (idx > 0);

		return ret;
	}

	/**
	 * Get the job comment
	 *
	 * @return job comment
	 */
	public String getComment() {
		final String sType = gets("Jobtag");

		if (sType == null)
			return null;

		if (sType.toLowerCase().startsWith("comment:"))
			return sType.substring(8).trim();

		return sType.trim();
	}

	/**
	 * Set the job comment
	 *
	 * @param comment
	 */
	public void setComment(final String comment) {
		final Collection<String> oldTag = getList("Jobtag");
		final Collection<String> newTag = new LinkedHashSet<>();

		if (oldTag != null) {
			for (final String s : oldTag)
				if (!s.startsWith("comment:"))
					newTag.add(s);

			clear("Jobtag");
		}

		for (final String s : newTag)
			append("Jobtag", s);

		if (comment != null)
			append("Jobtag", "comment:" + comment);
	}

	/**
	 * Get the (package, version) mapping. Ex: { (AliRoot -> v4-19-16-AN), (ROOT -> v5-26-00b-6) }
	 *
	 * @return packages
	 */
	@SuppressWarnings("unchecked")
	public Map<String, String> getPackages() {
		final Object o = get("Packages");

		if (!(o instanceof Collection))
			return null;

		final Iterator<String> it = ((Collection<String>) o).iterator();

		final Map<String, String> ret = new HashMap<>();

		while (it.hasNext()) {
			final String s = it.next();

			try {
				final int idx = s.indexOf('@');

				final int idx2 = s.indexOf("::", idx + 1);

				final String sPackage = s.substring(idx + 1, idx2);

				final String sVersion = s.substring(idx2 + 2);

				ret.put(sPackage, sVersion);
			}
			catch (final Throwable t) {
				System.err.println("Exception parsing package definition: " + s + " : " + t.getMessage());
			}
		}

		return ret;
	}

	/**
	 * Get the JDLVARIABLES. Ex: { LPMJobTypeId -> 1 }
	 *
	 * @return packages
	 */
	@SuppressWarnings("unchecked")
	public Map<String, Object> getJDLVariables() {
		final Object o = get("JDLVariables");

		if (!(o instanceof Collection))
			return null;

		final Iterator<String> it = ((Collection<String>) o).iterator();

		final Map<String, Object> ret = new HashMap<>();

		while (it.hasNext()) {
			final String s = it.next();
			try {
				ret.put(s, get(s));
			}
			catch (final Throwable t) {
				System.err.println("Exception parsing JDL variable definition: " + s + " : " + t.getMessage());
			}
		}
		return ret;
	}

	/**
	 * @param key
	 * @return the list for this key
	 */
	@SuppressWarnings("unchecked")
	public Collection<String> getList(final String key) {
		final Object o = get(key);

		if (o == null)
			return null;

		if (o instanceof Collection) {
			final Collection<String> c = (Collection<String>) o;

			if (c.size() > 0)
				return Collections.unmodifiableCollection(c);

			return null;
		}

		if (o instanceof CharSequence)
			return Arrays.asList(o.toString());

		return null;
	}

	/**
	 * Clear a list
	 *
	 * @param key
	 */
	public void clear(final String key) {
		final Object o = get(key);

		if (o == null)
			return;

		if (o instanceof Collection) {
			((Collection<?>) o).clear();
			return;
		}

		set(key, new LinkedHashSet<String>());
	}

	/**
	 * Get the base output directory, removing any #alien*# keywords from it
	 *
	 * @return output directory
	 */
	public String getOutputDir() {
		String s = gets("OutputDir");

		if (s == null)
			return null;

		final int idx = s.indexOf("#alien");

		if (idx >= 0) {
			final int idxEnd = s.indexOf("#", idx + 1);

			if (idxEnd > 0)
				s = s.substring(0, idx);
		}

		while (s.endsWith("/"))
			s = s.substring(0, s.length() - 1);

		return s;
	}

	/**
	 * Get the number of events/job in this simulation run
	 *
	 * @return events/job, of -1 if not supported
	 */
	public int getSimFactor() {
		String splitarguments = gets("SplitArguments");

		if (splitarguments == null || splitarguments.length() == 0)
			splitarguments = gets("Arguments");

		if (splitarguments != null && splitarguments.length() > 0) {
			final StringTokenizer st = new StringTokenizer(splitarguments, " \t");

			while (st.hasMoreTokens())
				if (st.nextToken().equals("--nevents") && st.hasMoreTokens())
					try {
						return Integer.parseInt(st.nextToken());
					}
					catch (final NumberFormatException nfe) {
						System.err.println("Could not parse the number of events from this line: " + splitarguments + " : " + nfe.getMessage());
					}
		}

		final List<String> inputFiles = getInputFiles();

		if (inputFiles == null)
			return -1;

		for (final String file : inputFiles)
			if (file.endsWith("sim.C")) {
				final int simFactor = getSimFactor(LFNUtils.getLFN(file));

				if (simFactor > 0)
					return simFactor;
			}

		return -1;
	}

	// void sim(Int_t nev=300) {
	// void sim(Int_t nev = 300) {
	private static final Pattern pSimEvents = Pattern.compile(".*void.*sim.*\\s+n(\\_)?ev\\s*=\\s*(\\d+).*");

	/**
	 * Get the number of events/job that this macro is expected to produce
	 *
	 * @param f
	 * @return events/job, or -1 if not supported
	 */
	public static int getSimFactor(final LFN f) {
		final GUID guid = GUIDUtils.getGUID(f);

		if (guid == null)
			return -1;

		final String sContent = IOUtils.getContents(guid);

		try {
			final BufferedReader br = new BufferedReader(new StringReader(sContent));

			String sLine;

			while ((sLine = br.readLine()) != null) {
				final Matcher m = pSimEvents.matcher(sLine);

				if (m.matches())
					return Integer.parseInt(m.group(2));
			}
		}
		catch (@SuppressWarnings("unused") final IOException ioe) {
			// ignore, cannot happen
		}

		return -1;
	}

	private static final String tab = " ";

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();

		final Map<String, Object> sorted = sortContent();

		for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
			if (sb.length() > 0)
				sb.append('\n');

			sb.append(tab).append(entry.getKey()).append(" = ");

			append(sb, entry.getValue());

			sb.append(";\n");
		}

		return sb.toString();
	}

	private static final void append(final StringBuilder sb, final Object o) {
		if (o instanceof StringBuilder || o instanceof StringBuffer || o instanceof Number)
			sb.append(o);
		else if (o instanceof Collection) {
			sb.append("{");

			final Collection<?> c = (Collection<?>) o;

			boolean first = true;

			for (final Object o2 : c) {
				if (!first)
					sb.append(",");
				else
					first = false;

				sb.append("\n").append(tab).append(tab).append("\"").append(o2).append("\"");
			}

			sb.append("\n").append(tab).append("}");
		}
		else
			sb.append('"').append(o.toString()).append('"');
	}

	/**
	 * Delete a key
	 *
	 * @param key
	 * @return the old value, if any
	 */
	public Object delete(final String key) {
		final Iterator<Map.Entry<String, Object>> it = jdlContent.entrySet().iterator();

		while (it.hasNext()) {
			final Map.Entry<String, Object> entry = it.next();

			if (entry.getKey().equalsIgnoreCase(key)) {
				it.remove();
				return entry.getValue();
			}
		}

		return null;
	}

	/**
	 * Set the value of a key. As value you can pass either:<br>
	 * <ul>
	 * <li>a String object, the value of which is to be put in quotes</li>
	 * <li>a StringBuilder object, then the content is set in the JDL without quotes (for example the Requirements field)</li>
	 * <li>a Collection, the values of which will be saved as an array of strings in the JDL</li>
	 * <li>a Number object, which will be saved without quotes</li>
	 * <li>any other Object, for which toString() will be called</li>
	 * </ul>
	 *
	 * @param key
	 *            JDL key name
	 * @param value
	 *            (new) value
	 * @return the previously set value, if any
	 */
	public Object set(final String key, final Object value) {
		if (value == null)
			return delete(key);

		final Object old = get(key);

		Object newValue;

		if (value instanceof Collection) {
			final LinkedHashSet<String> localCopy = new LinkedHashSet<>();

			for (final Object o : (Collection<?>) value)
				localCopy.add(StringFactory.get(o.toString()));

			newValue = localCopy;
		}
		else if (value instanceof String)
			newValue = StringFactory.get((String) value);
		else if (value instanceof StringBuilder)
			newValue = new StringBuilder(((StringBuilder) value).toString());
		else
			newValue = value;

		if (old != null) {
			for (final Map.Entry<String, Object> entry : jdlContent.entrySet())
				if (entry.getKey().equalsIgnoreCase(key)) {
					entry.setValue(newValue);
					break;
				}
		}
		else
			jdlContent.put(key, newValue);

		return old;
	}

	/**
	 * Append some String values to an array. If there is a previously set single value then it is transformed in an array and the previously set value is kept as the first entry of it.
	 *
	 * @param key
	 * @param value
	 */
	@SuppressWarnings("unchecked")
	public void append(final String key, final String... value) {
		if (key == null || value == null || value.length == 0)
			return;

		final Object old = get(key);

		final Collection<String> values;

		if (old == null) {
			values = new LinkedHashSet<>();
			jdlContent.put(key, values);
		}
		else if (old instanceof Collection)
			values = (Collection<String>) old;
		else {
			values = new LinkedHashSet<>();
			values.add(old.toString());

			boolean added = false;

			for (final Map.Entry<String, Object> entry : jdlContent.entrySet())
				if (entry.getKey().equalsIgnoreCase(key)) {
					added = true;
					entry.setValue(values);
					break;
				}

			if (!added)
				jdlContent.put(key, values);
		}

		for (final String s : value)
			values.add(StringFactory.get(s));
	}

	/**
	 * @param requirement
	 *            extra constraint to add to the job
	 * @return <code>true</code> if this extra requirement was added
	 */
	public final boolean addRequirement(final String requirement) {
		if (requirement == null || requirement.length() == 0)
			return false;

		final Object old = get("Requirements");

		final StringBuilder newValue;

		if (old != null) {
			if (old instanceof StringBuilder)
				newValue = (StringBuilder) old;
			else {
				newValue = new StringBuilder();
				newValue.append(getString(old));

				set("Requirements", newValue);
			}

			if (newValue.indexOf(requirement) >= 0)
				return false;

			if (newValue.length() > 0)
				newValue.append(" && ");
		}
		else {
			newValue = new StringBuilder();

			set("Requirements", newValue);
		}

		if (requirement.matches("^\\(.+\\)$"))
			newValue.append(requirement);
		else
			newValue.append("( ").append(requirement).append(" )");

		return true;
	}

	/**
	 * @return the HTML representation of this JDL
	 */
	public String toHTML() {
		final StringBuilder sb = new StringBuilder();

		final Map<String, Object> sorted = sortContent();

		for (final Map.Entry<String, Object> entry : sorted.entrySet()) {
			final String key = entry.getKey();

			sb.append("<DIV style='margin-bottom:6px'><B>").append(key).append("</B> = ");

			appendHTML(entry.getKey(), sb, entry.getValue());

			sb.append(";<BR></DIV>\n");
		}

		return sb.toString();
	}

	private static final void appendHTML(final String key, final StringBuilder sb, final Object o) {
		if (o instanceof StringBuilder || o instanceof StringBuffer || o instanceof Number) {
			if (o instanceof Number)
				sb.append("<font color=darkgreen>").append(o).append("</font>");
			else
				sb.append(formatExpression(o.toString()));
		}
		else if (o instanceof Collection) {
			sb.append("{<br><div style='padding-left:20px'>");

			final Collection<?> c = (Collection<?>) o;

			boolean first = true;

			for (final Object o2 : c) {
				if (!first)
					sb.append(",<br>");
				else
					first = false;

				String text = o2.toString();

				if (key.toLowerCase().startsWith("output") && !key.toLowerCase().equals("outputdir"))
					text = formatOutput(text);
				else if (key.equalsIgnoreCase("packages"))
					text = formatPackages(text);
				else if (key.equalsIgnoreCase("jobtag"))
					text = formatJobTag(text);
				else
					text = "<font color=navy>" + Format.escHtml(text) + "</font>";

				sb.append('"').append(text).append('"');
			}

			sb.append("</div>}");
		}
		else
			sb.append("\"<font color=navy>").append(o.toString()).append("</font>\"");
	}

	private static final String formatJobTag(final String text) {
		final int idx = text.indexOf("comment:");

		if (idx >= 0)
			return "<font color=navy>" + Format.escHtml(text.substring(0, idx + 8)) + "</font><font color=red><i>" + Format.escHtml(text.substring(8)) + "</i></font>";

		return text;
	}

	private static final Pattern PACKAGES = Pattern.compile("^\\w+@\\w+::[a-zA-Z0-9._-]+$");
	private static final Pattern NUMBER = Pattern.compile("(?<=(\\s|^))\\d+(.(\\d+)?)?(E[+-]\\d+)?(?=(\\s|$))");
	private static final Pattern JDLFIELD = Pattern.compile("(?<=\\Wother\\.)[A-Z][a-zA-Z]+(?=\\W)");

	/**
	 * @param sLine
	 * @param p
	 * @param sPreffix
	 * @param sSuffix
	 * @return formatted pattern
	 */
	public static String highlightPattern(final String sLine, final Pattern p, final String sPreffix, final String sSuffix) {
		final StringBuilder sb = new StringBuilder(sLine.length());

		final Matcher m = p.matcher(sLine);

		int iLastIndex = 0;

		while (m.find(iLastIndex)) {
			final String sMatch = sLine.substring(m.start(), m.end());

			sb.append(sLine.substring(iLastIndex, m.start()));
			sb.append(Format.replace(sPreffix, "${MATCH}", sMatch));
			sb.append(sMatch);
			sb.append(Format.replace(sSuffix, "${MATCH}", sMatch));

			iLastIndex = m.end();
		}

		sb.append(sLine.substring(iLastIndex));

		return sb.toString();
	}

	private static final String formatExpression(final String text) {
		String arg = highlightPattern(text, NUMBER, "<font color=darkgreen>", "</font>");
		arg = highlightPattern(arg, JDLFIELD, "<I>", "</I>");

		final StringBuilder sb = new StringBuilder();

		int old = 0;
		int idx = arg.indexOf('"');

		while (idx > 0) {
			final int idx2 = arg.indexOf('"', idx + 1);

			if (idx2 > idx) {
				sb.append(arg.substring(old, idx + 1));

				final String stringValue = arg.substring(idx + 1, idx2);

				if (PACKAGES.matcher(stringValue).matches())
					sb.append(formatPackages(stringValue));
				else
					sb.append("<font color=navy>").append(stringValue).append("</font>");

				sb.append('"');

				old = idx2 + 1;
				idx = arg.indexOf('"', old);
			}
			else
				break;
		}

		sb.append(arg.substring(old));

		return sb.toString();
	}

	private static final String formatPackages(final String arg) {
		String text = arg;

		final StringBuilder sb = new StringBuilder();

		int idx = text.indexOf('@');

		if (idx > 0) {
			sb.append("<font color=#999900>").append(Format.escHtml(text.substring(0, idx))).append("</font>@");
			text = text.substring(idx + 1);
		}

		idx = text.indexOf("::");

		if (idx > 0)
			sb.append("<font color=green>").append(Format.escHtml(text.substring(0, idx))).append("</font>::<font color=orange>").append(Format.escHtml(text.substring(idx + 2))).append("</font>");
		else
			sb.append(Format.escHtml(text));

		return sb.toString();
	}

	private static final String formatOutput(final String arg) {
		String text = arg;

		final StringBuilder sb = new StringBuilder();

		final int idx = text.indexOf(':');

		int idx2 = text.indexOf('@');

		if (idx > 0 && (idx2 < 0 || idx < idx2)) {
			sb.append("<font color=red>").append(Format.escHtml(text.substring(0, idx))).append("</font>:");
			text = text.substring(idx + 1);

			idx2 = text.indexOf('@');
		}

		if (idx2 >= 0)
			sb.append(Format.escHtml(text.substring(0, idx2 + 1))).append("<font color=green>").append(Format.escHtml(text.substring(idx2 + 1))).append("</font>");
		else
			sb.append(Format.escHtml(text));

		return sb.toString();
	}

	private static final List<String> correctTags = Arrays.asList("Arguments", "Executable", "GUIDFile", "InputBox", "InputData", "InputDataCollection", "InputDataList", "InputDataListFormat",
			"InputDownload", "InputFile", "JDLArguments", "JDLPath", "JDLProcessor", "JDLVariables", "JobLogOnClusterMonitor", "JobTag", "LPMActivity", "MasterJobID", "MemorySize", "OrigRequirements",
			"Output", "OutputArchive", "OutputDir", "OutputFile", "Packages", "Price", "Requirements", "SuccessfullyBookedPFNs", "TTL", "Type", "User", "ValidationCommand", "WorkDirectorySize",
			"Split", "SplitArguments", "SplitMaxInputFileNumber", "MasterJobID", "LPMParentPID", "LPMChainID", "MaxWaitingTime", "MaxFailFraction", "MaxResubmitFraction", "LegoResubmitZombies",
			"RunOnAODs", "LegoDataSetType", "LPMJobTypeID", "LPMAnchorRun", "LPMMetaData", "JDLArguments", "LPMRunNumber", "LPMAnchorProduction", "LPMProductionType", "LPMProductionTag",
			"LPMAnchorYear", "LPMInteractionType", "FilesToCheck", "OutputErrorE");

	private static final List<String> preferredOrder = Arrays.asList("user", "jobtag", "packages", "executable", "arguments", "inputfile", "inputdata", "inputdatalist", "inputdatalistformat",
			"inputdatacollection", "split", "splitmaxinputfilenumber", "splitarguments", "jdlpath", "jdlarguments", "jdlprocessor", "validationcommand", "outputdir", "output", "outputerrore",
			"outputarchive", "outputfile", "requirements", "origrequirements", "ttl", "price", "memorysize", "workdirectorysize", "masterjobid", "lpmparentpid", "lpmchainid", "lpmjobtypeid",
			"lpmactivity", "maxwaitingtime", "maxfailfraction", "maxresubmitfraction", "runonaods", "legodatasettype", "jdlvariables");

	private static final Map<String, String> correctedTags = new HashMap<>(correctTags.size());

	static {
		for (final String tag : correctTags)
			correctedTags.put(tag.toLowerCase(), tag);
	}

	/**
	 * @param tag
	 *            tag name, in lowercase!
	 */
	private static final String getCorrectedTag(final String tag, final String defaultValue) {
		final String s = correctedTags.get(tag);

		return s != null ? s : defaultValue;
	}

	private Map<String, Object> sortContent() {
		final LinkedHashMap<String, Object> ret = new LinkedHashMap<>(jdlContent.size());

		final Set<String> orderedTags = new LinkedHashSet<>();

		for (final String key : preferredOrder) {
			orderedTags.add(key);

			if (key.equals("jdlvariables")) {
				final Collection<String> variables = getList(key);

				if (variables != null)
					for (final String variable : variables)
						orderedTags.add(variable.toLowerCase());
			}
		}

		for (final String key : orderedTags) {
			String defaultKeyValue = key;
			Object value = null;

			for (final Map.Entry<String, Object> entry : jdlContent.entrySet())
				if (entry.getKey().equalsIgnoreCase(key)) {
					value = entry.getValue();
					defaultKeyValue = entry.getKey();
				}

			if (value != null)
				ret.put(getCorrectedTag(key, defaultKeyValue), value);
		}

		for (final Map.Entry<String, Object> entry : jdlContent.entrySet()) {
			final String lowerCaseKey = entry.getKey().toLowerCase();

			if (!orderedTags.contains(lowerCaseKey))
				ret.put(getCorrectedTag(lowerCaseKey, entry.getKey()), entry.getValue());
		}

		return ret;
	}

	/**
	 * Get the set of files (and patterns!) that the job is expected to register
	 *
	 * @param includeArchiveMembers
	 *            if <code>true</code> then archive member (patterns) are included
	 * @param excludeArchives
	 *            if <code>true</code> then archives will be skipped
	 * @return the set of files
	 */
	public Set<String> getOutputFileSet(final boolean includeArchiveMembers, final boolean excludeArchives) {
		return getOutputFileSet(null, includeArchiveMembers, excludeArchives);
	}

	/**
	 * Get the set of files (and patterns!) that the job is expected to register
	 * 
	 * @param tag if indicated then parse the given tag name instead of the default tag set (Output, OutputArchive, OutputFile)
	 *
	 * @param includeArchiveMembers
	 *            if <code>true</code> then archive member (patterns) are included
	 * @param excludeArchives
	 *            if <code>true</code> then archives will be skipped
	 * @return the set of files
	 */
	public Set<String> getOutputFileSet(final String tag, final boolean includeArchiveMembers, final boolean excludeArchives) {
		final List<String> outputFiles = tag != null ? getOutputFiles(tag) : getOutputFiles();

		final Set<String> ret = new TreeSet<>();

		if (outputFiles == null || outputFiles.size() == 0)
			return ret;

		for (String of : outputFiles) {
			int idx = of.indexOf('@');

			if (idx >= 0)
				of = of.substring(0, idx);

			idx = of.indexOf(':');

			if (idx >= 0) {
				if (!includeArchiveMembers && excludeArchives)
					continue;

				if (!includeArchiveMembers)
					of = of.substring(0, idx);
				else if (excludeArchives)
					of = of.substring(idx + 1);
			}

			final StringTokenizer st = new StringTokenizer(of, ":,");

			while (st.hasMoreTokens()) {
				final String tok = st.nextToken().trim();

				if (tok.length() > 0)
					ret.add(tok);
			}
		}

		return ret;
	}

	/**
	 * Remove the file patterns from outputFiles and add them as patterns to the outputPatterns object
	 *
	 * @param outputFiles
	 *            input, all file names and patterns, which is going to be altered by removing patterns from them
	 * @param outputPatterns
	 */
	public static void moveFilePatterns(final Set<String> outputFiles, final Set<Pattern> outputPatterns) {
		final Iterator<String> it = outputFiles.iterator();

		while (it.hasNext()) {
			String file = it.next();

			if (file.indexOf('*') >= 0) {
				file = Format.replace(file, ".", "\\.");
				file = Format.replace(file, "*", ".*");

				outputPatterns.add(Pattern.compile("^" + file + "$"));

				it.remove();
			}
		}
	}

	/**
	 * Check if a given file belongs to the output set
	 *
	 * @param outputFiles
	 * @param outputPatterns
	 * @param fileName
	 * @return <code>true</code> if the indicated file name belongs to the output
	 */
	public static boolean fileBelongsToOutput(final Set<String> outputFiles, final Set<Pattern> outputPatterns, final String fileName) {
		if (outputFiles != null && outputFiles.contains(fileName))
			return true;

		if (outputPatterns != null && outputPatterns.size() > 0)
			for (final Pattern p : outputPatterns)
				if (p.matcher(fileName).matches())
					return true;

		return false;
	}
}
