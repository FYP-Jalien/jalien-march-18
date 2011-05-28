package alien.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import alien.catalogue.FileSystemUtils;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.user.AliEnPrincipal;

/**
 * @author ron
 * @since May 28, 2011 implements AliEn whereis command
 * */
public class AlienCommandCompletePath extends AlienCommand {

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param al
	 *            all arguments received from SOAP request, contains user,
	 *            current directory and command
	 * @throws Exception
	 */
	public AlienCommandCompletePath(final AliEnPrincipal p,
			final ArrayList<Object> al) throws Exception {
		super(p, al);
	}

	/**
	 * @param p
	 *            AliEn principal received from https request
	 * @param sUsername
	 *            username received from SOAP request, can be different than the
	 *            one from the https request is the user make a su
	 * @param sCurrentDirectory
	 *            the directory from the user issued the command
	 * @param sCommand
	 *            the command requested through the SOAP request
	 * @param alArguments
	 *            command arguments, can be size 0 or null
	 * @throws Exception
	 */
	public AlienCommandCompletePath(final AliEnPrincipal p,
			final String sUsername, final String sCurrentDirectory,
			final String sCommand, final int iDebugLevel,
			final List<?> alArguments) throws Exception {
		super(p, sUsername, sCurrentDirectory, sCommand, iDebugLevel,
				alArguments);
	}

	/**
	 * @return a map of <String, List<String>> with only 2 keys
	 *         <ul>
	 *         <li>rcvalues - file list</li>
	 *         <li>rcmessages - file list with an extra \n at the end of the
	 *         file name</li>
	 *         </ul>
	 */
	@Override
	public HashMap<String, ArrayList<String>> executeCommand() {
		HashMap<String, ArrayList<String>> hmReturn = new HashMap<String, ArrayList<String>>();

		ArrayList<String> alrcValues = new ArrayList<String>();
		ArrayList<String> alrcMessages = new ArrayList<String>();

		// we got arguments for ls
		if (this.alArguments != null && this.alArguments.size() > 0)
			if (this.pAlienUser.canBecome(this.sUsername)) {

				for (Object oArg : this.alArguments) {
					String sArg = (String) oArg;

					System.out.println("We are asked to tabcomplete: " + sArg);
				}

				String search = (String) this.alArguments.toArray()[0];

				String abs = FileSystemUtils.getAbsolutePath(this.sUsername,
						this.sCurrentDirectory, search);

				System.out.println("tabcompleting on absolute path: " + abs);

				String foldername = abs.substring(0, abs.lastIndexOf("/"));

				String wildcard = abs.substring(abs.lastIndexOf("/") + 1,
						abs.length());
				System.out.println("tabcompleting wildcard: " + wildcard);

				String relpath = "";
				if (search.contains("/"))
					relpath = search.substring(0, search.lastIndexOf("/")+1);

				System.out
						.println("tabcompleting on foldername: " + foldername);

				final LFN folder = LFNUtils.getLFN(foldername);

				// what message in case of error?
				if (folder != null) {

					if (folder.type == 'd') {

						List<LFN> lLFN = folder.list();
						ArrayList<String> suggestions = new ArrayList<String>();

						for (LFN lfn : lLFN) {
							if (lfn.getFileName().startsWith(wildcard)) {
								System.out.println("matched wildcard: "
										+ lfn.getFileName());
								String suggest = lfn.getFileName();
								if (lfn.type == 'd')
									suggest += "/";
								suggestions.add(suggest);
							}
						}
						if (suggestions.size() == 1) {
							alrcValues.add(relpath + suggestions.get(0));
							String suggest = (relpath + suggestions.get(0))
									.replace(search, "");
							alrcMessages.add(suggest);

							System.out.println("filling in one: " + suggest);
						} else {
							for (String lfn : suggestions) {
								alrcValues.add(relpath + lfn);
								alrcMessages.add(relpath + lfn
										+ "\n");

							}
						}
					}

				}
			}
		hmReturn.put("rcvalues", alrcValues);
		hmReturn.put("rcmessages", alrcMessages);

		return hmReturn;

	}
}
