package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;

import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.se.SEUtils;

/**
 * @author ron
 * @since June 4, 2011
 */
public class JAliEnCommandaccess extends JAliEnBaseCommand {

	/**
	 * access request type: read,write,delete...
	 */
	private AccessType accessRequest = AccessType.NULL;

	/**
	 * access request lfnOrGUID
	 */
	private String lfnName = "";

	/**
	 * access request site
	 */

	private int referenceCount = 0;

	private final List<String> ses = new ArrayList<>();
	private final List<String> exses = new ArrayList<>();

	private final HashMap<String, Integer> qos = new HashMap<>();

	/**
	 * name of a local File that will be written
	 */
	private String localFileName = null;

	/**
	 * return pfns;
	 */
	private List<PFN> pfns = null;

	/**
	 * execute the access
	 */
	@Override
	public void run() {

		LFN lfn = null;
		GUID guid = null;
		boolean evenIfNotExists = false;

		if (accessRequest.equals(AccessType.WRITE)) {
			logger.log(Level.INFO, "Access called for a write operation");
			evenIfNotExists = true;
		}

		// obtaining LFN information for read or a new LFN for write
		lfn = commander.c_api.getLFN(lfnName, evenIfNotExists);

		if (lfn == null) {
			logger.log(Level.INFO, "Not able to retrieve LFN from Catalogue ");
			out.printErrln("Not able to retrieve LFN from Catalogue [error in processing].");
			return;
		}

		// is it ok here? to check, is this for a new file
		try {
			File f = null;

			if (localFileName != null && localFileName.length() > 0)
				f = new File(localFileName);

			if (f != null && f.exists() && f.isFile() && f.canRead())
				guid = GUIDUtils.createGuid(new File(localFileName), commander.user);
			else
				guid = GUIDUtils.createGuid(commander.user);
		} catch (final IOException e) {
			e.printStackTrace();

			// TODO
			return;
		}

		if (lfn.guid == null) {
			lfn.guid = guid.guid;
			lfn.size = guid.size;
			lfn.md5 = guid.md5;
			guid.lfnCache = new LinkedHashSet<>(1);
			guid.lfnCache.add(lfn);
		}

		if (accessRequest == AccessType.WRITE)
			pfns = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);
		else
			if (accessRequest == AccessType.READ) {
				logger.log(Level.INFO, "Access called for a read operation");
				pfns = commander.c_api.getPFNsToRead(lfn, ses, exses);
			}
			else {
				logger.log(Level.SEVERE, "Unknown access type");
				out.printErrln("Unknown access type [error in processing].");
			}

		if (pfns == null || pfns.size() < 1) {
			logger.log(Level.SEVERE, "Error getting the LFN/GUID");
			out.printErrln("Not able to get request LFN/GUID [error in processing].");
		}

		if (out.isRootPrinter()) {
			if (pfns != null && !pfns.isEmpty())
				for (final PFN pfn : pfns) {
					out.nextResult();

					if (!"alice::cern::setest".equals(commander.c_api.getSE(pfn.seNumber).getName().toLowerCase()))
						if (commander.c_api.getSE(pfn.seNumber).needsEncryptedEnvelope)
							out.setField("envelope", pfn.ticket.envelope.getEncryptedEnvelope());
						else
							out.setField("envelope", pfn.ticket.envelope.getSignedEnvelope());

					out.setField( "url", pfn.ticket.envelope.getTransactionURL());
					out.setField( "pfn", pfn.toString());
					out.setField("guid", pfn.getGuid().getName());
					out.setField(  "se", pfn.getSE().getName());
					out.setField("tags", pfn.getSE().qos.toString());
					out.setField("nSEs", String.valueOf(pfns.size()));
					out.setField("user", commander.user.getName());
				}
		}
		else
			if (pfns != null && !pfns.isEmpty())
				for (final PFN pfn : pfns) {
					out.printOutln(pfn.pfn);

					final SE se = SEUtils.getSE(pfn.seNumber);

					if (se != null)
						out.printOutln("SE: " + se.seName + " (" + (se.needsEncryptedEnvelope ? "needs" : "doesn't need") + " encrypted envelopes)");

					if (pfn.ticket != null) {
						final XrootDEnvelope env = pfn.ticket.envelope;

						if (env.getEncryptedEnvelope() != null)
							out.printOutln("Encrypted envelope:\n" + env.getEncryptedEnvelope());

						if (env.getSignedEnvelope() != null)
							out.printOutln("Signed envelope:\n" + env.getSignedEnvelope());
					}

					out.printOutln();
				}
			else
				out.printErrln("No PFNs for this LFN");
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		// ignore
	}

	/**
	 * get cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * serialize return values for gapi/root
	 *
	 * @return serialized return
	 */

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandaccess(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) {
		super(commander, out, alArguments);

		logger.log(Level.INFO, "Access arguments are " + alArguments);
		final java.util.ListIterator<String> arg = alArguments.listIterator();

		if (arg.hasNext()) {
			final String access = arg.next();
			logger.log(Level.INFO, "Access = " + access);
			if (access.startsWith("write")) {
				logger.log(Level.INFO, "We got write accesss");
				accessRequest = AccessType.WRITE;
			}
			else
				if (access.equals("read")) {
					logger.log(Level.INFO, "We got read accesss");
					accessRequest = AccessType.READ;
				}
				else
					logger.log(Level.INFO, "We got unknown accesss");

			if (!accessRequest.equals(AccessType.NULL) && (arg.hasNext())) {
				lfnName = arg.next();

				if (arg.hasNext()) {
					final StringTokenizer st = new StringTokenizer(arg.next(), ",");
					while (st.hasMoreElements()) {

						final String spec = st.nextToken();
						if (spec.contains("::")) {
							if (spec.indexOf("::") != spec.lastIndexOf("::"))
								if (spec.startsWith("!")) // an exSE spec
									exses.add(spec.toUpperCase().substring(1));
								else {// an SE spec
									ses.add(spec.toUpperCase());
									referenceCount++;
								}
						}
						else
							if (spec.contains(":"))
								try {
									final int c = Integer.parseInt(spec.substring(spec.indexOf(':') + 1));
									if (c > 0) {
										qos.put(spec.substring(0, spec.indexOf(':')), Integer.valueOf(c));
										referenceCount = referenceCount + c;
									}
									else
										throw new JAliEnCommandException("The number replicas has to be stricly positive");

								} catch (final Exception e) {
									throw new JAliEnCommandException("Exception parsing the QoS string", e);
								}
							else
								if (!spec.equals(""))
									throw new JAliEnCommandException();
					}
				}
			}
			else
				out.printErrln("Illegal Request type specified [error in request].");

		}
		else
			out.printErrln("No Request type specified [error in request].");

	}
}
