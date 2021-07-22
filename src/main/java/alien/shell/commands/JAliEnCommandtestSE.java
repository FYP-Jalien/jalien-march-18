package alien.shell.commands;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.PFNforReadOrDel;
import alien.api.catalogue.PFNforWrite;
import alien.catalogue.BookingTable.BOOKING_STATE;
import alien.catalogue.FileSystemUtils;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.access.AccessTicket;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.XrootDEnvelope;
import alien.io.protocols.Factory;
import alien.io.protocols.SpaceInfo;
import alien.io.protocols.TempFileManager;
import alien.io.protocols.Xrootd;
import alien.monitoring.Timing;
import alien.se.SE;
import alien.se.SEUtils;
import alien.shell.ErrNo;
import alien.shell.ShellColor;
import alien.user.JAKeyStore;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lazyj.Format;

/**
 *
 */
public class JAliEnCommandtestSE extends JAliEnBaseCommand {

	private boolean verbose = false;

	private boolean showCommand = false;

	private boolean showTiming = false;

	private final Set<SE> sesToTest = new LinkedHashSet<>();

	private static File getReferenceFile() throws IOException {
		// try some small files usually found on any system
		for (final String existingFile : new String[] { "/etc/hostname", "/etc/hosts", "/etc/timezone" }) {
			final File f = new File(existingFile);

			if (f.exists() && f.canRead())
				return f;
		}

		final File f = File.createTempFile("testSE", ".tmp");

		try (PrintWriter pw = new PrintWriter(f)) {
			pw.println("Some not so random content");
		}

		return f;
	}

	private static final String expected = " (" + ShellColor.jobStateGreen() + "expected" + ShellColor.reset() + ")";

	private static final String notOK = " (" + ShellColor.jobStateRed() + "NOT OK" + ShellColor.reset() + ")";

	private void afterCommandPrinting(final Timing t, final Xrootd xrootd) {
		if (showCommand)
			commander.printOutln(xrootd.getFormattedLastCommand());

		if (showTiming) {
			final double ms = t.getMillis();

			commander.printOut("Execution time: ");

			if (ms < 1000)
				commander.printOutln((long) ms + "ms");
			else if (ms < 10000)
				commander.printOutln(Format.point(ms / 1000) + "s");
			else
				commander.printOutln(Format.toInterval((long) ms));
		}
	}

	private void afterCommandPrinting(final Timing t, final String url) {
		if (showCommand)
			commander.printOutln("URL: " + url);

		if (showTiming) {
			final double ms = t.getMillis();

			commander.printOut("Execution time: ");

			if (ms < 1000)
				commander.printOutln((long) ms + "ms");
			else if (ms < 10000)
				commander.printOutln(Format.point(ms / 1000) + "s");
			else
				commander.printOutln(Format.toInterval((long) ms));
		}
	}

	private void openReadTest(final PFN pTarget, final Xrootd xrootd) {
		final AccessTicket oldTicket = pTarget.ticket;

		pTarget.ticket = null;

		try (Timing t = new Timing()) {
			commander.printOut("  Open read test: ");

			File tempFile = null;
			try {
				tempFile = xrootd.get(pTarget, null);
				commander.printOutln("reading worked" + notOK + " please check authorization configuration");
			}
			catch (final IOException ioe) {
				commander.printOutln("read back failed" + expected);

				if (verbose)
					commander.printOutln("    " + ioe.getMessage());
			}
			finally {
				if (tempFile != null) {
					TempFileManager.release(tempFile);
					tempFile.delete();
				}
			}

			afterCommandPrinting(t, xrootd);

			final String httpURL = pTarget.getHttpURL();

			if (httpURL != null) {
				commander.printOut("  Open HTTP read test: ");

				t.startTiming();

				try {
					downloadHTTP(httpURL);
					commander.printOutln("reading worked " + notOK + ", it should request an access envelope too");
				}
				catch (final IOException ioe) {
					commander.printOutln("read back failed " + expected);

					if (verbose)
						commander.printOutln("  " + ioe.getMessage());
				}

				afterCommandPrinting(t, httpURL);
			}
		}
		finally {
			pTarget.ticket = oldTicket;
		}
	}

	private static void downloadHTTP(final String address) throws IOException {
		final URL url = new URL(address);

		final URLConnection conn = url.openConnection();

		if ("https".equals(url.getProtocol())) {
			try {
				final SSLContext sc = SSLContext.getInstance("SSL");
				sc.init(null, JAKeyStore.trusts, new java.security.SecureRandom());

				((HttpsURLConnection) conn).setSSLSocketFactory(sc.getSocketFactory());
			}
			catch (NoSuchAlgorithmException | KeyManagementException ae) {
				throw new IOException("Cannot initialize SSL: " + ae.getMessage());
			}
		}

		conn.setConnectTimeout(30000);
		conn.setReadTimeout(120000);

		final byte[] buff = new byte[1024];

		try (InputStream is = conn.getInputStream()) {
			while (is.read(buff) > 0) {
				// consume all bytes from the source
			}
		}
	}

	private boolean openDeleteTest(final PFN pTarget, final Xrootd xrootd) {
		final AccessTicket oldTicket = pTarget.ticket;

		pTarget.ticket = null;

		try (Timing t = new Timing()) {
			commander.printOut("  Open delete test: ");

			try {
				if (xrootd.delete(pTarget, false)) {
					commander.printOutln("delete worked" + notOK);
					return true;
				}

				commander.printOutln("delete failed" + expected);
			}
			catch (final IOException ioe) {
				commander.printOutln("delete failed" + expected);

				if (verbose)
					commander.printOutln(ioe.getMessage());
			}

			afterCommandPrinting(t, xrootd);
		}
		finally {
			pTarget.ticket = oldTicket;
		}

		return false;
	}

	@Override
	public void run() {
		final File referenceFile;
		final GUID g;
		final LFN lfn;

		try {
			referenceFile = getReferenceFile();
			g = GUIDUtils.createGuid(referenceFile, commander.getUser());
			lfn = commander.c_api.getLFN(FileSystemUtils.getAbsolutePath(commander.user.getName(), commander.getCurrentDirName(), "testSE_" + System.currentTimeMillis()), true);
		}
		catch (final IOException ioe) {
			commander.setReturnCode(ErrNo.EIO, "Cannot pick a file to test with: " + ioe.getMessage());
			return;
		}

		lfn.guid = g.guid;
		lfn.size = g.size;
		lfn.md5 = g.md5;
		lfn.ctime = g.ctime;
		lfn.type = 'f';
		g.lfnCache = new LinkedHashSet<>(1);
		g.lfnCache.add(lfn);

		boolean openReadTested = false;
		boolean openDeleteTested = false;

		try {
			for (final SE se : sesToTest) {
				commander.printOutln(se.getName());
				PFNforWrite pfnForWrite;
				PFN pTarget;

				try {
					pfnForWrite = Dispatcher
							.execute(new PFNforWrite(commander.getUser(), commander.getSite(), lfn, g, Arrays.asList(se.getName()), null, new HashMap<>()));

					pTarget = pfnForWrite.getPFNs().iterator().next();
				}
				catch (final ServerException ex) {
					System.err.println("Could not get a write envelope for this entry: " + ex.getMessage());

					continue;
				}

				commander.printOut("  Open write test: ");

				final Xrootd xrootd = (Xrootd) Factory.xrootd.clone();

				boolean wasAdded = false;

				final AccessTicket writeTicket = pTarget.ticket;
				pTarget.ticket = null;

				@SuppressWarnings("resource")
				final Timing t = new Timing();

				try {
					xrootd.put(pTarget, referenceFile, false);

					commander.printOutln("could write, " + notOK + ", please check authorization configuration");

					wasAdded = true;

					commander.c_api.registerEnvelopes(Arrays.asList(writeTicket.envelope.getEncryptedEnvelope()), BOOKING_STATE.COMMITED);
				}
				catch (final IOException ioe) {
					commander.printOutln("cannot write" + expected);

					if (verbose)
						commander.printOutln("    " + ioe.getMessage());
				}

				afterCommandPrinting(t, xrootd);

				if (wasAdded) {
					openReadTest(pTarget, xrootd);

					openReadTested = true;

					openDeleteTest(pTarget, xrootd);

					openDeleteTested = true;
				}

				// now let's try this with the proper access tokens

				pTarget.ticket = writeTicket;

				commander.printOut("  Authenticated write test: ");

				wasAdded = false;

				t.startTiming();

				try {
					xrootd.put(pTarget, referenceFile);

					commander.printOutln("could write" + expected);

					wasAdded = true;

					commander.c_api.registerEnvelopes(Arrays.asList(writeTicket.envelope.getEncryptedEnvelope()), BOOKING_STATE.COMMITED);
				}
				catch (final IOException ioe) {
					commander.printOutln("cannot write " + notOK + "\n    " + ioe.getMessage());
				}

				afterCommandPrinting(t, xrootd);

				PFN infoPFN = pTarget;

				if (wasAdded) {
					// try to read back, first without a token (if it was not tested before), then with a token

					if (!openReadTested)
						openReadTest(pTarget, xrootd);

					commander.printOut("  Authenticated read: ");

					final List<PFN> readPFNs = commander.c_api.getPFNsToRead(lfn, Arrays.asList(se.getName()), null);

					if (readPFNs.size() > 0) {
						t.startTiming();

						infoPFN = readPFNs.iterator().next();

						final String httpURL = infoPFN.getHttpURL();

						File tempFile = null;
						try {
							tempFile = xrootd.get(infoPFN, null);
							commander.printOutln("file read back ok" + expected);
						}
						catch (final IOException ioe) {
							commander.printOutln("cannot read " + notOK + ":\n    " + ioe.getMessage());
						}
						finally {
							if (tempFile != null) {
								TempFileManager.release(tempFile);
								tempFile.delete();
							}
						}

						afterCommandPrinting(t, xrootd);

						if (httpURL != null) {
							t.startTiming();

							String authURL = httpURL + "?authz=" + XrootDEnvelope.urlEncodeEnvelope(infoPFN.ticket.envelope.getEncryptedEnvelope());

							commander.printOut("  Authenticated HTTP read access: ");
							try {
								downloadHTTP(authURL);
								commander.printOutln("reading worked " + expected);
							}
							catch (final IOException ioe) {
								commander.printOutln("read back failed " + notOK);

								if (verbose)
									commander.printOutln("  " + ioe.getMessage());
							}

							afterCommandPrinting(t, authURL);
						}
					}
					else {
						commander.printOutln("no PFNs to access");
					}
				}

				if ((wasAdded && !openDeleteTested) && openDeleteTest(pTarget, xrootd)) {
					wasAdded = false;

					commander.printOutln("  The file is gone, trying to add it back and then try the authenticated delete");

					t.startTiming();

					try {
						xrootd.put(pTarget, referenceFile);

						wasAdded = true;
					}
					catch (final IOException ioe) {
						commander.printOutln("    add operation failed, cannot test authenticated delete");

						if (verbose)
							commander.printOutln("      " + ioe.getMessage());
					}

					afterCommandPrinting(t, xrootd);
				}

				if (wasAdded) {
					// if the file is (still) on the SE, try to delete it with a proper token
					commander.printOut("  Authenticated delete: ");

					t.startTiming();

					try {
						final PFNforReadOrDel del = Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), null, AccessType.DELETE, lfn, Arrays.asList(se.getName()), null));

						final PFN delPFN = del.getPFNs().iterator().next();

						if (xrootd.delete(delPFN))
							commander.printOutln("delete worked ok" + expected);
					}
					catch (final IOException ioe) {
						commander.printOutln("couldn't delete " + notOK + ":\n    " + ioe.getMessage());
					}
					catch (final ServerException e) {
						commander.printOutln("couldn't get a delete token " + notOK + ":\n    " + e.getMessage());
					}

					afterCommandPrinting(t, xrootd);
				}

				commander.printOutln("  Space information:");

				t.startTiming();

				try {
					infoPFN.pfn = se.generateProtocol();
					infoPFN.ticket = null;

					final SpaceInfo info = xrootd.getSpaceInfo(infoPFN);

					commander.printOutln(info.toString());
				}
				catch (final IOException e) {
					commander.printOutln("    Could not get the space information due to: " + e.getMessage());
				}

				afterCommandPrinting(t, xrootd);

				commander.printOutln("  LDAP information:");

				commander.printOutln(se.toString());
			}
		}
		finally {
			commander.c_api.removeLFN(lfn.getCanonicalName());
		}
	}

	@Override
	public void printHelp() {
		commander.printOutln("Test the functional status of Grid storage elements");
		commander.printOutln("Usage: testSE [options] <some SE names, numbers or @tags>");
		commander.printOutln(helpOption("-v", "verbose error messages even when the operation is expected to fail"));
		commander.printOutln(helpOption("-c", "show full command line for each test"));
		commander.printOutln(helpOption("-t", "time each operation"));
		commander.printOutln(helpOption("-a", "test all SEs (obviously a very long operation)"));
	}

	@Override
	public boolean canRunWithoutArguments() {
		return false;
	}

	/**
	 * @param commander
	 * @param alArguments
	 * @throws OptionException
	 */
	public JAliEnCommandtestSE(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("v");
			parser.accepts("c");
			parser.accepts("t");
			parser.accepts("a");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			verbose = options.has("v");
			showCommand = options.has("c");
			showTiming = options.has("t");

			if (options.has("a")) {
				sesToTest.addAll(commander.c_api.getSEs(null));
			}
			else {
				for (final Object o : options.nonOptionArguments()) {
					final String s = o.toString();

					if (s.indexOf("@") == 0) {
						final String tag = s.substring(1);

						for (final SE se : commander.c_api.getSEs(null))
							if (se.isQosType(tag))
								sesToTest.add(se);

						continue;
					}

					try {
						final int seNumber = Integer.parseInt(s);
						final SE se = SEUtils.getSE(seNumber);

						if (se != null) {
							sesToTest.add(se);
						}
						else {
							commander.printErrln("No such SE: " + seNumber);
							commander.setReturnCode(ErrNo.ENOENT);
							setArgumentsOk(false);
							return;
						}
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						final SE se = SEUtils.getSE(s);

						if (se != null) {
							sesToTest.add(se);
						}
						else {
							commander.printErrln("No such SE: " + s);
							commander.setReturnCode(ErrNo.ENOENT);
							setArgumentsOk(false);
							return;
						}
					}
				}
			}
		}
		catch (final OptionException | IllegalArgumentException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
			return;
		}

		if (sesToTest.isEmpty())
			setArgumentsOk(false);
	}
}
