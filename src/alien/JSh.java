package alien;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

import alien.api.JBoxServer;
import alien.config.ConfigUtils;
import alien.config.JAliEnIAm;
import alien.shell.BusyBox;
import alien.shell.ShellColor;
import alien.shell.commands.JAliEnBaseCommand;
import lazyj.commands.CommandOutput;
import lazyj.commands.SystemCommand;
import lia.util.process.ExternalProcess.ExitStatus;
import utils.ProcessWithTimeout;

/**
 * @author ron
 * @since Jun 21, 2011
 */
public class JSh {

	static {
		ConfigUtils.getVersion();
	}

	/**
	 * name of the OS the shell is running in
	 */
	static String osName;

	/**
	 *
	 */
	static BusyBox boombox = null;

	private static boolean color = true;

	/**
	 * enable color output mode
	 */
	public static void color() {
		color = true;
	}

	/**
	 * disable color output mode
	 */
	public static void blackwhite() {
		color = false;
	}

	/**
	 * is color output mode enabled
	 *
	 * @return enabled or not
	 */
	public static boolean doWeColor() {
		return color;
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		osName = getOsName();

		try {
			sun.misc.Signal.handle(new sun.misc.Signal("INT"), new sun.misc.SignalHandler() {
				@Override
				public void handle(final sun.misc.Signal sig) {
					if (boombox != null)
						boombox.callJBoxGetString("SIGINT");
				}
			});
		} catch (@SuppressWarnings("unused") final Throwable t) {
			// ignore if not on a SUN VM
		}

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (boombox != null)
					if (boombox.prompting()) {
						if (appendOnExit)
							System.out.println("exit");
						JSh.printGoodBye();
					}
			}
		});

		if (args.length > 0 && (("-h".equals(args[0])) || ("-help".equals(args[0])) || ("--h".equals(args[0])) || ("--help".equals(args[0])) || ("help".equals(args[0]))))
			printHelp();
		else
			if (args.length > 0 && ("-k".equals(args[0])))
				JSh.killJBox();
			else {

				if (!JSh.JBoxRunning())
					if (runJBox())
						try {
							int a = 10;
							while (a < 5000) {
								Thread.sleep(a);
								if (JSh.JBoxRunning())
									break;
								a = a * 2;
							}
						} catch (@SuppressWarnings("unused") final InterruptedException e) {
							// ignore
						}

				if (JSh.JBoxRunning()) {
					if (args.length > 0 && "-e".equals(args[0])) {
						color = false;
						boombox = new BusyBox(addr, port, password);

						if (boombox != null) {
							final StringTokenizer st = new StringTokenizer(joinSecondArgs(args), ",");

							while (st.hasMoreTokens())
								boombox.callJBox(st.nextToken().trim());
						}
						else
							printErrConnJBox();

					}
					else
						boombox = new BusyBox(addr, port, password, user, true);
				}
				else
					printErrNoJBox();
			}
	}

	/**
	 * Trigger no 'exit\n' to be written out on exit
	 */
	static boolean appendOnExit = true;

	private static String getOsName() {
		return System.getProperty("os.name");
	}

	/**
	 * Trigger no 'exit\n' to be written out on exit
	 */
	public static void noAppendOnExit() {
		appendOnExit = false;
	}

	/**
	 * Trigger no 'exit\n' to be written out on exit
	 */
	public static void appendOnExit() {
		appendOnExit = true;
	}

	private static boolean runJBox() {
		Process p;

		try {
			p = Runtime.getRuntime().exec(new String[] { "java", "-Duserid=" + System.getProperty("userid"), "-DAliEnConfig=" + System.getProperty("AliEnConfig"), "-server",
					"-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"), "alien.JBox" });

		} catch (final IOException ioe) {
			System.err.println("Error starting jBox : " + ioe.getMessage());
			return false;
		}

		try (BufferedReader out = new BufferedReader(new InputStreamReader(p.getInputStream())); PrintWriter pw = new PrintWriter(p.getOutputStream())) {
			Console cons;

			if ((cons = System.console()) != null) {
				pw.println(new String(cons.readPassword("[%s]", "Grid certificate password: ")));
				pw.flush();
			}
			else
				System.err.println("Error getting console.");

		} catch (final Exception e) {
			System.err.println("Error asking for password : " + e.getMessage());
			p.destroy();
			return false;
		}

		String sLine;

		try (BufferedReader err = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
			if ((sLine = err.readLine()) != null) {
				if (sLine.equals(JBoxServer.passACK)) {
					System.out.println();
					return true;
				}
				System.err.println(sLine);
				return false;
			}

		} catch (final IOException ioe) {
			System.err.println("Error starting jBox: " + ioe.getMessage());
			return false;
		} finally {
			try {
				p.getOutputStream().close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// ignore
			}
			try {
				p.getInputStream().close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// ignore
			}
			try {
				p.getErrorStream().close();
			} catch (@SuppressWarnings("unused") final IOException e) {
				// ignore
			}
		}
		return false;
	}

	private static final String kill = "/bin/kill";
	// private static final String fuser = "/bin/fuser";

	private static String addr;
	private static String user;
	private static String password;
	private static int port = 0;
	private static int pid = 0;

	/*
	 * private static void startJBox() {
	 * if (!JSh.JBoxRunning()) {
	 * 
	 * // APIServer.startJBox();
	 * 
	 * final List<String> command = new ArrayList<>();
	 * 
	 * command.add("nohup");
	 * command.add("./run.sh");
	 * 
	 * command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
	 * 
	 * final String confDir = System.getProperty("AliEnConfig");
	 * 
	 * if (confDir != null && confDir.length() > 0)
	 * command.add("-DAliEnConfig=" + confDir);
	 * 
	 * command.add("alien.JBox");
	 * command.add("&");
	 * 
	 * final ProcessBuilder pBuilder = new ProcessBuilder(command);
	 * 
	 * pBuilder.redirectErrorStream(false);
	 * 
	 * // try {
	 * // pBuilder.start();
	 * // } catch (Exception e) {
	 * // e.printStackTrace();
	 * // System.err.println("Could not start JBox.");
	 * // }
	 * System.out.println();
	 * }
	 * JSh.getJBoxPID();
	 * }
	 */

	private static void killJBox() {
		if (JSh.JBoxRunning()) {

			final ProcessBuilder pBuilder = new ProcessBuilder(new String[] { kill, String.valueOf(pid) });

			pBuilder.redirectErrorStream(true);
			final ExitStatus exitStatus;
			try {
				final Process p = pBuilder.start();
				final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
				pTimeout.waitFor(2, TimeUnit.SECONDS);
				exitStatus = pTimeout.getExitStatus();

				if (exitStatus.getExtProcExitStatus() == 0)
					System.out.println("[" + pid + "] JBox killed.");
				else
					System.err.println("Could not kill the JBox, PID:" + pid);

			} catch (final Throwable e) {
				System.err.println("Could not kill the JBox, PID:" + pid + " : " + e.getMessage());
			}
		}
		else
			System.out.println("We didn't find any JBox running.");
	}

	private static boolean JBoxRunning() {
		if (JSh.getJBoxPID()) {

			if (osName.startsWith("Mac")) {
				final CommandOutput co = SystemCommand.bash("ps -p " + pid, false);

				return co.stdout.contains("alien.JBox");
			}

			if (System.getProperty("os.name").equals("Linux")) {
				final File f = new File("/proc/" + pid + "/cmdline");
				if (f.exists()) {
					String buffer = "";
					try (BufferedReader fi = new BufferedReader(new InputStreamReader(new FileInputStream(f)))) {
						buffer = fi.readLine();
					} catch (@SuppressWarnings("unused") final IOException e) {
						return false;
					}

					if (buffer != null && buffer.contains("alien.JBox"))
						return true;
				}

			}
		}

		return false;
	}

	private static boolean getJBoxPID() {

		final File f = new File(new File(System.getProperty("java.io.tmpdir")), "jclient_token_" + System.getProperty("userid"));

		if (f.exists()) {
			final byte[] buffer = new byte[(int) f.length()];
			try (BufferedInputStream fi = new BufferedInputStream(new FileInputStream(f))) {
				fi.read(buffer);
			} catch (@SuppressWarnings("unused") final IOException e) {
				port = 0;
				return false;
			}

			final String[] specs = new String(buffer).split("\n");

			for (final String spec : specs) {
				final String[] kval = spec.split("=");

				if (("Host").equals(kval[0].trim()))
					addr = kval[1].trim();
				else
					if (("Port").equals(kval[0].trim()))
						try {
							port = Integer.parseInt(kval[1].trim());
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							port = 0;
						}
					else
						if (("PID").equals(kval[0].trim()))
							try {
								pid = Integer.parseInt(kval[1].trim());
							} catch (@SuppressWarnings("unused") final NumberFormatException e) {
								pid = 0;
							}
						else
							if (("Passwd").equals(kval[0].trim()))
								password = kval[1].trim();
							else
								if (("User").equals(kval[0].trim()))
									user = kval[1].trim();
			}
			return true;
		}
		// else
		// System.err.println("Token file does not exists.");

		return false;
	}

	/**
	 * @return BusyBox of JSh
	 * @throws IOException
	 */
	public static BusyBox getBusyBox() throws IOException {
		getJBoxPID();
		return new BusyBox(addr, port, password);
	}

	/**
	 * @return PID of JBox
	 */
	public static int getPID() {
		return pid;
	}

	/**
	 * @return port of JBox
	 */
	public static int getPort() {
		return port;
	}

	/**
	 * @return addr of JBox
	 */
	public static String getAddr() {
		return addr;
	}

	/**
	 * @return pass of JBox
	 */
	public static String getPassword() {
		return password;
	}

	/**
	 * reconnect
	 *
	 * @return success
	 */
	public static boolean reconnect() {

		return JSh.JBoxRunning();

	}

	private static String joinSecondArgs(final String[] args) {
		final StringBuilder ret = new StringBuilder();

		for (int a = 1; a < args.length; a++)
			ret.append(args[a]).append(' ');

		return ret.toString();
	}

	private static void printErrNoJBox() {
		printErr("JBox isn't running, so we won't start JSh.");
	}

	private static void printErrConnJBox() {
		printErr("Error connecting JBox.");
	}

	/**
	 * @param message
	 */
	public static void printErr(final String message) {
		if (color)
			System.err.println(ShellColor.errorMessage() + message + ShellColor.reset());
		else
			System.err.println(message);
	}

	/**
	 * @param message
	 */
	public static void printOut(final String message) {
		if (color)
			System.out.println(ShellColor.infoMessage() + message + ShellColor.reset());
		else
			System.err.println(message);
	}

	private static void printHelp() {
		System.out.println(JAliEnIAm.whatsMyFullName());
		System.out.println("Have a cup! Cheers, ACS");
		System.out.println();
		System.out.println(JAliEnBaseCommand.helpUsage("jsh", "[-options]"));
		System.out.println(JAliEnBaseCommand.helpStartOptions());
		System.out.println(JAliEnBaseCommand.helpOption("-e <cmd>[,<cmd>]", "execute directly a comma separated list of commands"));
		System.out.println(JAliEnBaseCommand.helpOption("-h | -help", "this help"));
		System.out.println();
		System.out.println(JAliEnBaseCommand.helpParameter("more info to come."));
		System.out.println();

	}

	/**
	 * be polite
	 */
	public static void printGoodBye() {
		JSh.printOut("GoodBye.");
	}

}
