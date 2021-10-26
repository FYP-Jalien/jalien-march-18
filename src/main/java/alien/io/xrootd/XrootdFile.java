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
		final StringTokenizer st = new StringTokenizer(line);

		if (st.countTokens() != 5)
			throw new IllegalArgumentException("Not in the correct format : " + line);

		perms = st.nextToken();

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
