package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;

import alien.catalogue.CatalogEntity;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.se.SE;
import alien.shell.ErrNo;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

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
	 * return pfns;
	 */
	private List<PFN> pfns = null;

	/**
	 * For write envelopes the size, if known, can be embedded in the booking request
	 */
	private long size = -1;

	/**
	 * For write envelopes the MD5 checksum, if known, can be embedded in the booking request
	 */
	private String md5sum = null;

	/**
	 * For write envelopes you can also pass the job ID, when applicable
	 */
	private long jobId = -1;

	/**
	 * Filter the SEs for read command
	 */
	private boolean filter = false;

	/**
	 * Flag to print http(s) URLs where available, and urlencoded envelopes for them
	 */
	private boolean httpURLs = false;

	/**
	 * execute the access
	 */
	@Override
	public void run() {
		boolean evenIfNotExists = false;

		if (accessRequest.equals(AccessType.WRITE)) {
			logger.log(Level.FINER, "Access called for a write operation");
			evenIfNotExists = true;
		}

		// obtaining LFN information for read or a new LFN for write
		final LFN lfn = lfnName.startsWith("/") ? commander.c_api.getLFN(lfnName, evenIfNotExists) : null;

		final CatalogEntity referenceEntity;

		if (lfn == null) {
			GUID referenceGUID = null;

			if (accessRequest == AccessType.READ && GUIDUtils.isValidGUID(lfnName)) {
				referenceGUID = commander.c_api.getGUID(lfnName);
			}

			if (referenceGUID == null) {
				logger.log(Level.WARNING, "Not able to retrieve LFN from catalogue: " + lfnName);
				commander.setReturnCode(ErrNo.ENOENT, lfnName);
				return;
			}

			referenceEntity = referenceGUID;
		}
		else
			referenceEntity = lfn;

		if (accessRequest == AccessType.WRITE) {
			if (lfn == null) {
				commander.setReturnCode(ErrNo.ENOENT, "Could not get the LFN to create/modify: " + lfnName);
				return;
			}

			final GUID guid;
			if (!lfn.exists || lfn.guid == null) {
				guid = GUIDUtils.createGuid(commander.user);

				if (size >= 0)
					guid.size = size;

				if (md5sum != null)
					guid.md5 = md5sum;

				if (jobId >= 0)
					lfn.jobid = jobId;

				lfn.guid = guid.guid;
				lfn.size = guid.size;
				lfn.md5 = guid.md5;
			}
			else {
				// check if the details match the existing entry, if they were provided
				if ((size >= 0 && lfn.size != size) || (md5sum != null && lfn.md5 != null && !md5sum.equalsIgnoreCase(lfn.md5)) || (jobId >= 0 && lfn.jobid >= 0 && jobId != lfn.jobid)) {
					commander.setReturnCode(ErrNo.EINVAL, "You seem to want to write a different file from the existing one in the catalogue");
					return;
				}

				guid = commander.c_api.getGUID(lfn.guid.toString(), evenIfNotExists, false);

				if (guid == null) {
					commander.setReturnCode(ErrNo.EUCLEAN, "Could not retrieve the GUID entry for the existing file");
					return;
				}

				if (md5sum != null) {
					// could the existing entries be enhanced?
					if (lfn.md5 == null)
						lfn.md5 = md5sum;

					if (guid.md5 == null)
						guid.md5 = md5sum;
				}

				if (jobId >= 0 && lfn.jobid < 0)
					lfn.jobid = jobId;
			}

			guid.addKnownLFN(lfn);

			pfns = commander.c_api.getPFNsToWrite(lfn, guid, ses, exses, qos);
		}
		else if (accessRequest == AccessType.READ) {
			logger.log(Level.FINE, "Access called for a read operation");
			pfns = commander.c_api.getPFNsToRead(referenceEntity, ses, exses);

			if (pfns != null && filter) {
				final Iterator<PFN> it = pfns.iterator();

				while (it.hasNext()) {
					final PFN p = it.next();

					if (p == null) {
						it.remove();
						continue;
					}

					final SE se = p.getSE();

					if (se == null) {
						it.remove();
						continue;
					}

					if (exses.contains(se.seName.toUpperCase())) {
						it.remove();
						continue;
					}

					if (ses.size() > 0 && !ses.contains(se.seName.toUpperCase())) {
						it.remove();
						continue;
					}
				}
			}
		}
		else {
			logger.log(Level.SEVERE, "Unknown access type");
			commander.setReturnCode(ErrNo.EINVAL, accessRequest.toString());
			return;
		}

		if (pfns == null || pfns.isEmpty()) {
			logger.log(Level.SEVERE, "No " + accessRequest.toString() + " PFNs for this entity: " + referenceEntity.getName() + " / " + ses + " / NOT " + exses);
			commander.setReturnCode(ErrNo.EBADFD, "No " + accessRequest.toString() + " PFNs");
			return;
		}

		for (final PFN pfn : pfns) {
			commander.outNextResult();
			commander.printOutln(httpURLs ? pfn.getHttpURL() : pfn.pfn);
			final SE se = commander.c_api.getSE(pfn.seNumber);

			if (se != null) {
				commander.printOutln("SE: " + se.seName + " (" + (se.needsEncryptedEnvelope ? "needs" : "doesn't need") + " encrypted envelopes)");

				if (pfn.ticket != null) {
					final XrootDEnvelope env = pfn.ticket.envelope;

					if (!"alice::cern::setest".equals(se.getName().toLowerCase()))
						if (se.needsEncryptedEnvelope) {
							String envelope = env.getEncryptedEnvelope();

							if (httpURLs)
								envelope = XrootDEnvelope.urlEncodeEnvelope(envelope);

							commander.printOut("envelope", envelope);
							commander.printOutln("Encrypted envelope:\n" + envelope);
						}
						else {
							commander.printOut("envelope", env.getSignedEnvelope());
							commander.printOutln("Signed envelope:\n" + env.getSignedEnvelope());
						}

					// If archive member access requested, add it's filename as anchor
					final String archiveAnchorName = pfn.ticket.envelope.getArchiveAnchorFileName();
					if (archiveAnchorName != null) {
						commander.printOut("url", pfn.ticket.envelope.getTransactionURL() + "#" + archiveAnchorName);
					}
					else {
						commander.printOut("url", pfn.ticket.envelope.getTransactionURL());
					}

					commander.printOut("guid", pfn.getGuid().getName());
					commander.printOut("se", se.getName());
					commander.printOut("tags", se.qos.toString());
					commander.printOut("nSEs", String.valueOf(pfns.size()));
					commander.printOut("md5", referenceEntity.getMD5());
					commander.printOut("size", String.valueOf(referenceEntity.getSize()));
					commander.printOutln();
				}
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("access", "[options] <read|write> <lfn> [<specs>]"));
		commander.printOutln(helpOption("-s", "for write requests, size of the file to be uploaded, when known"));
		commander.printOutln(helpOption("-m", "for write requests, MD5 checksum of the file to be uploaded, when known"));
		commander.printOutln(helpOption("-j", "for write requests, the job ID that created these files, when applicable"));
		commander.printOutln(helpOption("-f", "for read requests, filter the SEs based on the given specs list"));
		commander.printOutln(helpOption("-u", "for read requests, print http(s) URLs where available, and the envelopes in urlencoded format"));
		commander.printOutln();
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
	 *
	 * @param alArguments
	 *            the arguments of the command
	 */
	public JAliEnCommandaccess(final JAliEnCOMMander commander, final List<String> alArguments) {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("s").withRequiredArg().ofType(Long.class);
			parser.accepts("j").withRequiredArg().ofType(Long.class);
			parser.accepts("m").withRequiredArg();
			parser.accepts("f");
			parser.accepts("u");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			final java.util.ListIterator<String> arg = optionToString(options.nonOptionArguments()).listIterator();

			if (arg.hasNext()) {
				final String access = arg.next();
				logger.log(Level.FINE, "Access = " + access);
				if (access.startsWith("write")) {
					accessRequest = AccessType.WRITE;
				}
				else if (access.equals("read")) {
					accessRequest = AccessType.READ;
				}
				else
					logger.log(Level.SEVERE, "We got unknown accesss request: " + access);

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
							else {
								int separatorIdx = spec.indexOf(':');

								if (separatorIdx < 0)
									separatorIdx = spec.indexOf('=');

								if (separatorIdx > 0)
									try {
										final int c = Integer.parseInt(spec.substring(separatorIdx + 1).trim());
										if (c > 0) {
											qos.put(spec.substring(0, separatorIdx).trim(), Integer.valueOf(c));
											referenceCount = referenceCount + c;
										}
										else {
											commander.setReturnCode(ErrNo.EINVAL, "The number replicas has to be stricly positive in `" + spec + "`");
											setArgumentsOk(false);
											return;
										}
									}
									catch (@SuppressWarnings("unused") final Exception e) {
										commander.setReturnCode(ErrNo.EINVAL, "Invalid format of the QoS string `" + spec + "`");
										setArgumentsOk(false);
										return;
									}
								else if (!spec.isBlank()) {
									commander.setReturnCode(ErrNo.EINVAL, "Don't know what to do with `" + spec + "`");
									setArgumentsOk(false);
									return;
								}
							}
						}
					}
				}
				else {
					commander.setReturnCode(ErrNo.EINVAL, "Invalid access type requested: " + access);
					setArgumentsOk(false);
					return;
				}
			}

			if (options.has("s"))
				size = ((Long) options.valueOf("s")).longValue();

			if (options.has("m"))
				md5sum = options.valueOf("m").toString();

			if (options.has("j"))
				jobId = ((Long) options.valueOf("j")).longValue();

			filter = options.has("f") && (ses.size() > 0 || exses.size() > 0);

			httpURLs = options.has("u");
		}
		catch (final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}
}
