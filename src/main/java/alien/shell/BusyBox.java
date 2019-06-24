package alien.shell;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Arrays;
import java.util.StringTokenizer;

import alien.JSh;
import alien.api.JBoxServer;
import alien.config.JAliEnIAm;
import alien.shell.commands.JShPrintWriter;
import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.StringsCompleter;
import lazyj.Format;

/**
 * @author ron
 * @since Feb, 2011
 */
public class BusyBox {

	private final static int tryreconnect = 1;

	private int remainreconnect = tryreconnect;

	private static final String noSignal = String.valueOf((char) 33);

	private static int pender = 0;
	private static boolean pending = false;
	private static final String[] pends = { ".   ", " .  ", "  . ", "   ." };

	private static final String promptPrefix = JAliEnIAm.whatsMyName() + JAliEnIAm.myJShPrompt() + " ";

	private static final String promptColorPrefix = ShellColor.boldBlack() + JAliEnIAm.whatsMyName() + ShellColor.reset() + JAliEnIAm.myJShPrompt() + " ";

	private static final String promptSuffix = " > ";

	private static int commNo = 1;

	private ConsoleReader reader;
	private final PrintWriter out;

	private boolean prompting = false;

	private String username;
	private String role;

	private String currentDir;

	/**
	 *
	 * @return the current directory
	 */
	public String getCurrentDir() {
		return currentDir;
	}

	/**
	 * print welcome
	 */
	public void welcome() {
		System.out.println("Hello " + username + ",");
		System.out.println();
		System.out.println("Welcome to " + JAliEnIAm.whatsMyFullName());
		System.out.println("Have a cup! Cheers, ACS");
		System.out.println();
	}

	private Socket s = null;

	private InputStream is;

	private BufferedReader br;

	private OutputStream os;

	private static String toXML(final String... commandAndArguments) {
		final StringBuilder sb = new StringBuilder();

		sb.append("<document>\n");
		sb.append("<command>");
		sb.append(Format.escHtml(commandAndArguments[0]));
		sb.append("</command>\n");

		if (commandAndArguments.length > 1)
			for (int i = 1; i < commandAndArguments.length; i++) {
				sb.append("<o>");
				sb.append(Format.escHtml(commandAndArguments[i]));
				sb.append("</o>\n");
			}

		sb.append("</document>\n");

		return sb.toString();
	}

	private void sendCommand(final String... commandAndArguments) throws IOException {
		final String xml = toXML(commandAndArguments);

		os.write(xml.getBytes());
		os.flush();
	}

	private boolean connect(final String addr, final int port, final String password) {

		if (addr != null && port != 0 && password != null)
			try {
				s = new Socket(addr, port);

				is = s.getInputStream();
				br = new BufferedReader(new InputStreamReader(is));

				os = s.getOutputStream();

				sendCommand("password", password);

				final byte[] passACK = new byte[JBoxServer.passACK.length()];

				final int read = is.read(passACK);

				if (read != 9 || !"OKPASSACK".equals(new String(passACK)))
					return false;

				return (!noSignal.equals(callJBoxGetString("setshell jaliensh")));
			}
			catch (@SuppressWarnings("unused") final IOException e) {
				return false;
			}
		return false;
	}

	private boolean reconnect() {
		if (remainreconnect < 1) {
			printErrShutdown();
			callJBox("shutdown", false);
			Runtime.getRuntime().exit(1);
		}
		remainreconnect--;

		final String whoami = this.username;
		final String roleami = this.role;
		final String currentDirTemp = this.currentDir;

		if (JSh.reconnect())
			if (connect(JSh.getAddr(), JSh.getPort(), JSh.getPassword()))
				if (callJBox("user " + whoami))
					if (callJBox("role " + roleami))
						if (callJBox("cd " + currentDirTemp)) {
							remainreconnect = tryreconnect;
							return true;
						}

		printErrRestartJBox();
		return false;

	}

	/**
	 * the JAliEn busy box
	 *
	 * @param addr
	 * @param port
	 * @param password
	 *
	 * @throws IOException
	 */
	public BusyBox(final String addr, final int port, final String password) throws IOException {
		this(addr, port, password, null, false);

	}

	/**
	 * the JAliEn busy box
	 *
	 * @param addr
	 * @param port
	 * @param password
	 * @param username
	 * @param startPrompt
	 *
	 * @throws IOException
	 */
	public BusyBox(final String addr, final int port, final String password, final String username, final boolean startPrompt) throws IOException {

		out = new PrintWriter(System.out);

		if (startPrompt) {
			this.username = username;
			welcome();
			out.flush();
			prompting = true;
		}

		if (!connect(addr, port, password)) {
			printInitConnError();
			throw new IOException();
		}

		if (startPrompt)
			new Thread() {
				@Override
				public void run() {
					try {
						prompt();
					}
					catch (final IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}.start();
	}

	private static String genPromptPrefix() {
		if (JSh.doWeColor())
			return promptColorPrefix;

		return promptPrefix;
	}

	/**
	 * loop the prompt for the user
	 *
	 * @throws IOException
	 */
	public void prompt() throws IOException {

		String line;

		reader = new ConsoleReader();
		reader.setBellEnabled(false);
		reader.setExpandEvents(false);
		// reader.setDebug(new PrintWriter(new FileWriter("writer.debug", true)));
		reader.addCompleter(new ArgumentCompleter(new StringsCompleter(callJBoxGetString("commandlist").split("\\s+")), new GridLocalFileCompletor(this)));

		String prefixCNo = "0";
		while ((line = reader.readLine(genPromptPrefix() + "[" + prefixCNo + commNo + "] " + currentDir + promptSuffix)) != null) {

			if (commNo == 9)
				prefixCNo = "";

			out.flush();
			line = line.trim();

			if (line.equalsIgnoreCase("quit") || line.equalsIgnoreCase("exit")) {
				JSh.noAppendOnExit();
				break;
			}

			executeCommand(line);
			commNo++;

		}
	}

	/**
	 * @param line
	 * @return response from JBox
	 */
	public String callJBoxGetString(final String line) {
		final String sline = line;

		checkColorSwitch(sline);

		do
			try {

				if (socketThere(s)) {
					sendCommand(sline.split("\\s+"));

					final StringBuilder ret = new StringBuilder();
					String sLine = null;
					boolean signal = false;

					while ((sLine = br.readLine()) != null) {
						signal = true;
						if (sLine.startsWith(JShPrintWriter.outputterminator))
							updateEnvironment(sLine);
						else
							if (sLine.endsWith(JShPrintWriter.streamend))
								break;
							else {
								if (ret.length() > 0)
									ret.append('\n');

								ret.append(sLine);
							}
					}

					if (signal)
						return ret.toString();
				}

			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// e.printStackTrace();
			}
		while (reconnect());

		printConnError();
		return noSignal;
	}

	/**
	 * @param line
	 * @return success of the call
	 */
	public boolean callJBox(final String line) {
		return callJBox(line, true);
	}

	private boolean callJBox(final String line, final boolean tryReconnect) {
		final String sline = line;

		checkColorSwitch(sline);

		do
			try {

				if (socketThere(s)) {
					sendCommand(sline.split("\\s+"));

					String sLine;
					boolean signal = false;

					while ((sLine = br.readLine()) != null) {

						signal = true;

						if (JShPrintWriter.pendSignal.equals(sLine)) {
							pending = true;
							// System.out.write(("\rI/O ["+ pends[pender] +
							// "]").getBytes());
							System.out.print("\rI/O [" + pends[pender] + "]");
							// out.flush();
							pender++;
							if (pender >= pends.length)
								pender = 0;
							continue;
						}

						if (pending) {
							pending = false;
							pender = 0;
							// System.out.write("\r".getBytes());
							System.out.print("\r                                        \r");
						}

						if (sLine.startsWith(JShPrintWriter.errTag))
							JSh.printErr("Error: " + sLine.substring(1));
						else
							if (sLine.startsWith(JShPrintWriter.outputterminator))
								updateEnvironment(sLine);
							else
								if (sLine.endsWith(JShPrintWriter.lineTerm))
									break;
								else {
									out.println(sLine);
									out.flush();
								}

					}

					if (signal)
						return true;
				}

			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// ignore
				// e.printStackTrace();
			}
		while (tryReconnect && reconnect());

		if (tryReconnect)
			printConnError();
		return false;
	}

	private void updateEnvironment(final String env) {

		final StringTokenizer st = new StringTokenizer(env.substring(1), JShPrintWriter.fieldseparator);

		if (st.hasMoreTokens())
			currentDir = st.nextToken();
		if (st.hasMoreTokens())
			username = st.nextToken();
		if (st.hasMoreTokens())
			role = st.nextToken();
	}

	/**
	 * execute a command
	 *
	 * @param callLine
	 *            arguments of the command, first one is the command
	 */
	public void executeCommand(final String callLine) {

		// String args[] = callLine.split(SpaceSep);
		final String args[] = callLine.split("\\s+");

		if (args[0].length() > 0)
			if (args[0].equals(".")) {
				final StringBuilder command = new StringBuilder();

				for (int c = 1; c < args.length; c++)
					command.append(args[c]).append(' ');

				syscall(command.toString());
			}
			else
				if (args[0].equals("gbbox")) {
					final StringBuilder command = new StringBuilder("alien -s -e ");
					for (int c = 1; c < args.length; c++)
						command.append(args[c]).append(' ');

					syscall(command.toString());
				}
				else
					if (FileEditor.isEditorCommand(args[0])) {
						if (args.length == 2)
							editCatalogueFile(args[0], args[1]);
						else {
							out.println(args[0] + " requires an LFN argument");
							out.flush();
						}
					}
					else
						if (args[0].equals("shutdown"))
							shutdown();
						else
							if (args[0].equals("help"))
								usage();
							else
								callJBox(callLine);
	}

	/**
	 * print some help message
	 */
	public void usage() {
		out.println("JAliEn Grid Client, started in 2010, Version: " + JAliEnIAm.whatsVersion());
		out.println("Press <tab><tab> to see the available commands.");
	}

	/**
	 * do a call to the underlying system shell
	 */
	private static void syscall(final String command) {

		String line;
		Process p = null;
		Runtime rt;
		try {
			rt = Runtime.getRuntime();
			p = rt.exec(command);

			try (BufferedReader brCleanUp = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
				while ((line = brCleanUp.readLine()) != null)
					System.out.println(line);
			}

			try (BufferedReader brCleanUp = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
				while ((line = brCleanUp.readLine()) != null)
					System.out.println(line);
			}
		}
		catch (final Exception e) {
			System.out.println(e);
		}
	}

	/**
	 * true once running the prompt
	 *
	 * @return are we running a prompt
	 */
	public boolean prompting() {
		return prompting;
	}

	private static void checkColorSwitch(final String line) {
		if ("blackwhite".equals(line))
			JSh.blackwhite();

		else
			if ("color".equals(line))
				JSh.color();
	}

	private static boolean socketThere(final Socket s) {
		return (!s.isClosed() && s.isBound() && s.isConnected() && !s.isInputShutdown() && !s.isOutputShutdown());
	}

	private void shutdown() {
		try {
			System.out.print("Shutting down jBox...");
			if (socketThere(s)) {
				sendCommand("shutdown");

				// TODO: How to tell that jBox was killed successfully
				// if(socketThere(s))
				System.out.println("DONE.");
				// else{
				// System.out.println("ERROR.");
				// System.out.println("JBox might still be running.");
				// }
			}
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			// e.printStackTrace();
		}
		JSh.printGoodBye();
		System.exit(0);
	}

	private void editCatalogueFile(final String editcmd, final String LFNName) {

		final String ret = callJBoxGetString("cp -t " + LFNName.trim());

		final StringTokenizer st = new StringTokenizer(ret, "\n");

		String localFile = null;
		while (st.hasMoreTokens()) {
			final String a = st.nextToken();
			if (a.contains("Downloaded file to ")) {
				localFile = a.replace("Downloaded file to ", "").trim();
				break;
			}
		}

		if (localFile == null) {
			JSh.printErr("Error getting the file.");
			return;
		}

		final long lastMod = (new File(localFile)).lastModified();

		FileEditor editor = null;

		try {
			editor = new FileEditor(editcmd);
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			JSh.printErr("The editor [" + editcmd + "] was not found on your system.");
			return;
		}

		editor.edit(localFile);

		if ((new File(localFile)).lastModified() != lastMod) {
			String parent;
			String fileName;
			if (LFNName.contains("/")) {
				parent = LFNName.substring(0, LFNName.lastIndexOf('/')) + "/";
				fileName = LFNName.substring(LFNName.lastIndexOf('/') + 1);
			}
			else {
				parent = "";
				fileName = LFNName;
			}

			// we cannot allow "/..file", only hidde out with "/.file"
			if (!fileName.startsWith("."))
				fileName = "." + fileName;

			callJBox("rm -silent " + parent + fileName + "~");

			callJBox("mv " + LFNName + " " + parent + fileName + "~");

			callJBox("cp -S disk=4 file:" + localFile + " " + LFNName);
		}
	}

	private static void printInitConnError() {
		JSh.printErr("Could not connect to JBox.");
	}

	private static void printErrShutdown() {
		JSh.printErr("Shutting down...");
	}

	private static void printErrRestartJBox() {
		JSh.printErr("JBox seems to be dead, please restart it.");
	}

	private static void printConnError() {
		JSh.printErr("Connection to JBox interrupted.");
	}
}
