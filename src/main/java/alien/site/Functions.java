package alien.site;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import alien.test.utils.TestCommand;
import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author ron
 * @since Sep 09, 2011
 */
public class Functions {

	/**
	 * @param commands
	 * @param verbose
	 * @return success of the shell executions
	 */
	public static boolean execShell(final ArrayList<TestCommand> commands, final boolean verbose) {
		boolean state = true;
		for (final TestCommand c : commands) {
			if (verbose)
				c.verbose();
			state = c.exec() && state;
			if (!state)
				break;
		}
		return state;
	}

	/**
	 * @param dir
	 * @return if it could create the directory
	 */
	public static boolean makeDirs(final String dir) {

		if (!(new File(dir).mkdirs())) {
			System.out.println("Could not create directory: " + dir);
			return false;
		}
		return true;
	}

	/**
	 * @param s
	 *            command, single space separated
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(final String s) {
		return callGetStdOut(s, false);
	}

	/**
	 * @param s
	 *            command, single space separated
	 * @param verbose
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(final String s, final boolean verbose) {
		final TestCommand c = new TestCommand(s);
		if (verbose)
			c.verbose();
		c.exec();
		return c.getStdOut();
	}

	/**
	 * @param s
	 *            command, single space separated
	 * @return stdout of executed command
	 */
	public static String callGetStdOut(final String[] s) {

		final TestCommand c = new TestCommand(s);
		c.exec();
		return c.getStdOut();
	}

	/**
	 * Write String[] as a file, with String[0] as file name and String[1] as file content
	 *
	 * @param file
	 * @throws Exception
	 */
	public static void writeOutFile(final String[] file) throws Exception {
		writeOutFile(file[0], file[1]);
	}

	/**
	 * @param file_name
	 * @param content
	 * @throws Exception
	 */
	public static void writeOutFile(final String file_name, final String content) throws Exception {
		try (Writer out = new OutputStreamWriter(new FileOutputStream(file_name))) {
			out.write(content);
		}
	}

	/**
	 * @param command
	 * @return full location of the command
	 */
	public static String which(final String command) {

		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(Arrays.asList("which", command));

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(7, TimeUnit.SECONDS);
		try {
			final ExitStatus exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0)
				return exitStatus.getStdOut().trim();

		}
		catch (@SuppressWarnings("unused") final Exception e) {
			// ignore
		}
		System.err.println("Command [" + command + "] not found.");
		return null;

	}

	/**
	 * @param path_with_env
	 * @return path without env vars
	 */
	public static String resolvePathWithEnv(final String path_with_env) {
		try {
			final String[] path_splitted = path_with_env.split("/");
			String path_resolved = "";
			for (String dir : path_splitted) {
				path_resolved += '/';
				if (dir.startsWith("$")) { // it's an env variable
					dir = System.getenv(dir.substring(1));
				}
				path_resolved += dir;
			}
			if (path_resolved.startsWith("//")) {
				path_resolved = path_resolved.substring(1);
			}
			return path_resolved;
		}
		catch (@SuppressWarnings("unused") final NullPointerException e) {
			return null;
		}
	}

	/**
	 * @param zip
	 * @param extractTo
	 * @throws IOException
	 */
	public static final void unzip(final File zip, final File extractTo) throws IOException {

		try (ZipFile archive = new ZipFile(zip)) {
			final Enumeration<? extends ZipEntry> e = archive.entries();
			while (e.hasMoreElements()) {
				final ZipEntry entry = e.nextElement();
				final File file = new File(extractTo, entry.getName());
				if (entry.isDirectory() && !file.exists()) {
					if (!file.mkdirs())
						System.err.println("Cannot create base directory: " + file);
				}
				else {
					if (!file.getParentFile().exists()) {
						final File f = file.getParentFile();

						if (f.exists()) {
							if (!f.isDirectory())
								System.err.println("File exists but is not a directory: " + f);
						}
						else if (!f.mkdirs())
							System.err.println("Cannot create directory: " + f);
					}

					try (InputStream in = archive.getInputStream(entry); BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file))) {
						final byte[] buffer = new byte[8192];
						int read;

						while (-1 != (read = in.read(buffer)))
							out.write(buffer, 0, read);
					}
				}
			}
		}
	}
}
