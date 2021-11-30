 
package alien.api.catalogue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Request;
import alien.catalogue.BookingTable;
import alien.catalogue.LFN;
import alien.config.ConfigUtils;
import alien.site.OutputEntry;
import alien.user.AliEnPrincipal;

/**
 * 
 *
 */
public class BookArchiveEntries extends Request {
	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(BookArchiveEntries.class.getCanonicalName());

	private static final long serialVersionUID = -3932544423316312626L;
	private final OutputEntry entry;
	private final LFN entry_lfn;
	private final String outputDir;
	private boolean wasBooked;

	/**
	 * @param entry archive entry
	 * @param entry_lfn archive lfn
	 * @param outputDir absolute path
	 * @param user
	 */
	public BookArchiveEntries(final OutputEntry entry, final LFN entry_lfn, final String outputDir, final AliEnPrincipal user) {
		setRequestUser(user);
		this.entry = entry;
		this.entry_lfn = entry_lfn;
		this.outputDir = outputDir;
		this.wasBooked = false;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.outputDir, this.entry != null ? this.entry.toString() : null);
	}

	@Override
	public void run() {
		if (entry != null && outputDir != null && outputDir.length() != 0) {
			try {
				wasBooked = BookingTable.bookArchiveContents(entry, entry_lfn, outputDir, getEffectiveRequester());
			}
			catch (final IOException e) {
				logger.log(Level.SEVERE, "Could not book entry " + entry.getName());
				e.printStackTrace();
			}
		}
		else {
			logger.log(Level.SEVERE, "Invalid arguments in BookArchiveEntries");
		}
	}

	/**
	 * @return the status of booking
	 */
	public boolean wasBooked() {
		return this.wasBooked;
	}
}
