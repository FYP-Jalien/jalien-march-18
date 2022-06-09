package alien.io.xrootd;

import java.util.Date;
import java.util.StringTokenizer;

import lazyj.Format;

/**
 * @author costing
 *
 */
public class XrootdFile implements Comparable<XrootdFile> {
	/**
	 * entry permissions
	 */
	public final String perms;

	/**
	 * size
	 */
	public final long size;

	/**
	 * change time
	 */
	public final Date date;

	/**
	 * full path
	 */
	public final String path;

	/**
	 * parse the output of "ls", "dirlist" or "dirlistrec" and extract the tokens
	 *
	 * @param line
	 * @throws IllegalArgumentException
	 */
	public XrootdFile(final String line) throws IllegalArgumentException {
		// drwx 2016-01-29 07:45:36 57 /15/34485
		// -rw-r----- aliecsgc def-cg 135399544 2021-09-15 14:13:23 /eos/aliceo2/ls2data/GC/ECS/2021-09-15_15-56/run0501294_2021-09-15T15_57_47Z/2021-09-15_00040415.tf
		final StringTokenizer st = new StringTokenizer(line);

		final int tokens = st.countTokens();

		if (tokens != 5 && tokens != 7)
			throw new IllegalArgumentException("Not in the correct format : " + line);

		perms = st.nextToken();

		if (tokens == 7) {
			// skip over owner and group names
			st.nextToken();
			st.nextToken();
		}

		final String t2 = st.nextToken();
		final String t3 = st.nextToken();
		final String t4 = st.nextToken();

		path = st.nextToken();

		long lsize;

		String datePart;

		try {
			lsize = Long.parseLong(t2);

			datePart = t3 + " " + t4;
		}
		catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
			lsize = Long.parseLong(t4);

			datePart = t2 + " " + t3;
		}

		if (isFile() && (lsize < 0 || lsize > 1024L * 1024 * 1024 * 100)) {
			System.err.println("XrootdFile: Negative or excessive size detected: " + lsize + ", from " + line);
			lsize = 1;
		}

		try {
			date = Format.parseDate(datePart);
		}
		catch (final NumberFormatException nfe) {
			System.err.println("Could not parse date `" + datePart + "` of `" + line + "`");
			throw new IllegalArgumentException("Date not in a parseable format `" + datePart + "`", nfe);
		}

		if (date == null)
			throw new IllegalArgumentException("Could not parse date `" + datePart + "`");

		size = lsize;
	}

	/**
	 * @return true if dir
	 */
	public boolean isDirectory() {
		return perms.startsWith("d");
	}

	/**
	 * @return true if file
	 */
	public boolean isFile() {
		return perms.startsWith("-");
	}

	/**
	 * @return the last token of the path
	 */
	public String getName() {
		final int idx = path.lastIndexOf('/');

		if (idx >= 0)
			return path.substring(idx + 1);

		return path;
	}

	@Override
	public int compareTo(final XrootdFile o) {
		final int diff = perms.compareTo(o.perms);

		if (diff != 0)
			return diff;

		return path.compareTo(o.path);
	}

	@Override
	public boolean equals(final Object obj) {
		if (obj == null || !(obj instanceof XrootdFile))
			return false;

		return compareTo((XrootdFile) obj) == 0;
	}

	@Override
	public int hashCode() {
		return path.hashCode();
	}

	@Override
	public String toString() {
		return path;
	}
}
