package alien.site;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import alien.io.IOUtils;

/**
 * @author Miguel
 * @since Apr 19, 2016
 */
public class OutputEntry implements Serializable {
	private static final long serialVersionUID = -3731415996541086683L;

	private final String name;
	private final ArrayList<String> filesIncluded;
	private final HashMap<String, String> md5members;
	private final HashMap<String, Long> sizemembers;
	private final String options;
	private final Long queueId;
	private final boolean isArchive;
	private boolean isRootArchive;
	private String asyncReplication = "";

	/**
	 *
	 */
	public OutputEntry() {
		this.name = null;
		this.filesIncluded = null;
		this.options = null;
		this.queueId = null;
		this.isArchive = false;
		this.isRootArchive = false;
		this.md5members = new HashMap<>();
		this.sizemembers = new HashMap<>();
	}

	/**
	 * @param name
	 * @param filesIncluded
	 * @param options
	 * @param jobid
	 * @param isArchive
	 */
	public OutputEntry(final String name, final ArrayList<String> filesIncluded, final String options, final Long jobid, final boolean isArchive) {
		this.name = name;
		this.filesIncluded = filesIncluded;

		this.queueId = jobid;
		this.isArchive = isArchive;
		this.isRootArchive = false;
		this.md5members = new HashMap<>();
		this.sizemembers = new HashMap<>();

		if (this.filesIncluded != null)
			for (final String f : this.filesIncluded)
				if (f.endsWith(".root")) {
					this.isRootArchive = true;
					break;
				}

		String syncOptions = "";

		// parse options
		if (options.length() > 0) {
			final String[] opts = options.split(",");

			for (final String o : opts) {
				if (o.startsWith("*")) {
					if (asyncReplication.length() > 0)
						asyncReplication += ",";

					asyncReplication += o.substring(1);
				}
				else {
					if (syncOptions.length() > 0)
						syncOptions += ",";

					syncOptions += o;
				}
			}
		}

		this.options = syncOptions;
	}

	/**
	 * @return entry name
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * @return job ID
	 */
	public Long getQueueId() {
		return this.queueId;
	}

	/**
	 * @return the async replication targets, if any
	 */
	public String getAsyncTargets() {
		return asyncReplication;
	}

	/**
	 * @return archive member to file size mapping
	 */
	public HashMap<String, Long> getSizesIncluded() {
		return this.sizemembers;
	}

	/**
	 * @return archive member to MD5 checksum mapping
	 */
	public HashMap<String, String> getMD5sIncluded() {
		return this.md5members;
	}

	/**
	 * @return list of included files in this archive, or empty list if a file
	 */
	public ArrayList<String> getFilesIncluded() {
		return isArchive() ? this.filesIncluded : new ArrayList<>();
	}

	/**
	 * @return options
	 */
	public String getOptions() {
		return this.options;
	}

	/**
	 * @return <code>true</code> if this is an archive
	 */
	public boolean isArchive() {
		return this.filesIncluded != null && this.filesIncluded.size() > 0 || isArchive;
	}

	/**
	 * @param folderPath
	 * @return list of members
	 */
	public ArrayList<String> createZip(final String folderPath) {
		String path = folderPath;

		if (path == null)
			path = System.getProperty("user.dir");
		if (!path.endsWith("/"))
			path += "/";

		if (this.filesIncluded == null)
			return null;

		try (ZipOutputStream out = new ZipOutputStream(new FileOutputStream(path + this.name))) {
			// output file
			if (this.isRootArchive)
				out.setMethod(ZipOutputStream.STORED);

			boolean hasPhysicalFiles = false;

			for (final String file : this.filesIncluded) {
				final File f = new File(path + file);
				if (!f.exists() || !f.isFile() || !f.canRead() || f.length() <= 0) {
					// filesIncluded.remove(file);
					System.err.println("File " + file + " doesn't exist or cannot be read!. Is directory then?: " + f.isDirectory() + ". File after all?: " + f.isFile());
					// throw new NullPointerException("File " + file + " for archive " + this.name + " doesn't exist or cannot be read!");
					continue;
				}
				hasPhysicalFiles = true;

				String md5 = null;
				try {
					md5 = IOUtils.getMD5(f);
				}
				catch (final Exception e1) {
					System.err.println("Error calculating md5 of: " + file + ": " + e1.getMessage());
					// filesIncluded.remove(file);
					continue;
				}

				// Save md5 and size
				sizemembers.put(file, Long.valueOf(f.length()));
				md5members.put(file, md5);

				// input file
				try (FileInputStream in = new FileInputStream(path + file)) {
					// name of the file inside the zip file
					final ZipEntry entry = new ZipEntry(file);

					if (this.isRootArchive) {
						final File fToAdd = new File(path + file);
						entry.setSize(fToAdd.length());
						entry.setCompressedSize(fToAdd.length());
						entry.setCrc(getCRC32(fToAdd));
					}

					out.putNextEntry(entry);

					final byte[] b = new byte[1024];
					int count;

					while ((count = in.read(b)) > 0)
						out.write(b, 0, count);
				}
			}

			// Only keep files with md5
			filesIncluded.retainAll(md5members.keySet());

			if (!hasPhysicalFiles)
				Files.delete(Paths.get(path + this.name));

		}
		catch (final FileNotFoundException e) {
			e.printStackTrace();
		}
		catch (final IOException e) {
			e.printStackTrace();
		}

		return filesIncluded;
	}

	private static long getCRC32(final File fToAdd) throws FileNotFoundException, IOException {
		final CRC32 crc32 = new CRC32();

		final byte[] buff = new byte[8192];

		try (FileInputStream fis = new FileInputStream(fToAdd)) {
			int len;

			while ((len = fis.read(buff)) > 0)
				crc32.update(buff, 0, len);
		}

		return crc32.getValue();
	}

	@Override
	public String toString() {
		String toString = "Name: " + this.name + " Options: " + this.options + " Files?: ";
		if (this.filesIncluded != null)
			toString += this.filesIncluded.toString();
		return toString;
	}
}
