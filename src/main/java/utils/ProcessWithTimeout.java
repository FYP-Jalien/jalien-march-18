package utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import lia.util.process.ExternalProcess.ExecutorFinishStatus;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author costing
 * @since 2016-10-05
 */
public final class ProcessWithTimeout {

	private static final class CopyThread extends Thread {
		private final InputStream is;
		private final StringBuilder sb;

		private boolean active = true;

		public CopyThread(final InputStream is, final StringBuilder sb, final String title) {
			this.is = is;
			this.sb = sb;
			setName(title);
			setDaemon(true);
		}

		@Override
		public void run() {
			final byte[] buffer = new byte[1024];

			while (active) {
				int count;
				try {
					count = is.read(buffer);

					if (count < 0)
						active = false;

					if (count > 0)
						sb.append(new String(buffer, 0, count));

				}
				catch (@SuppressWarnings("unused") final IOException e) {
					active = false;
				}
			}
		}

		public void close() throws InterruptedException {
			active = false;

			while (isAlive())
				// wait for the thread to get the point and exit
				Thread.sleep(1);

			try {
				// drain any leftover bytes from the stream
				if (is.available() > 0) {
					final byte[] buffer = new byte[1024];

					int count;
					do {
						count = is.read(buffer);

						if (count > 0)
							sb.append(new String(buffer, 0, count));
					} while (count >= 0);
				}
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				// ignore
			}
			finally {
				try {
					is.close();
				}
				catch (@SuppressWarnings("unused") final IOException ioe) {
					// ignore
				}
			}
		}
	}

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(ProcessWithTimeout.class.getCanonicalName());

	private final Process p;

	private final StringBuilder sbOut = new StringBuilder();
	private final StringBuilder sbErr = new StringBuilder();

	private final String command;

	private final CopyThread stdoutThread;
	private final CopyThread stderrThread;

	/**
	 * Wrap the process with an output reading thread and helpers for timeout operations
	 *
	 * @param p
	 * @param pBuilder
	 */
	@SuppressWarnings("resource")
	public ProcessWithTimeout(final Process p, final ProcessBuilder pBuilder) {
		this.p = p;
		this.command = pBuilder.command().toString();

		String title = " - " + command;

		if (title.length() > 100)
			title = title.substring(0, 100);

		try {
			p.getOutputStream().close();
		}
		catch (@SuppressWarnings("unused") final IOException e) {
			// ignore
		}

		stdoutThread = new CopyThread(p.getInputStream(), sbOut, "stdout" + title);
		stdoutThread.start();

		if (pBuilder.redirectErrorStream()) {
			stderrThread = null;

			try {
				p.getErrorStream().close();
			}
			catch (@SuppressWarnings("unused") final IOException ioe) {
				// ignore
			}
		}
		else {
			stderrThread = new CopyThread(p.getErrorStream(), sbErr, "stderr" + title);
			stderrThread.start();
		}
	}

	/**
	 * @return stdout of the process
	 */
	public StringBuilder getStdout() {
		return sbOut;
	}

	/**
	 * @return stderr of the process
	 */
	public StringBuilder getStderr() {
		return sbErr;
	}

	private boolean exitedOk = false;

	private int exitValue = -1;

	private boolean shouldTerminate = false;

	/**
	 * @param timeout
	 * @param unit
	 * @return <code>true</code> if the process exited on its own, or <code>false</code> if it was killed forcibly
	 * @throws InterruptedException
	 */
	public boolean waitFor(final long timeout, final TimeUnit unit) throws InterruptedException {
		try {
			exitedOk = p.waitFor(timeout, unit);

			if (exitedOk)
				exitValue = p.exitValue();
			else {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "Forcibly terminating command: " + command);

				p.destroyForcibly();
			}
		}
		finally {
			shouldTerminate = true;

			if (stdoutThread != null)
				stdoutThread.close();

			if (stderrThread != null)
				stderrThread.close();
		}

		return exitedOk;
	}

	/**
	 * @return command exit value
	 */
	public int exitValue() {
		return exitValue;
	}

	/**
	 * @return <code>true</code> if the process exited on its own or <code>false</code> if it was killed
	 */
	public boolean exitedOk() {
		return exitedOk;
	}

	@Override
	public String toString() {
		if (shouldTerminate) {
			if (exitedOk)
				return "Process exited normally with code " + exitValue;

			return "Process was forcefully terminated";
		}

		return "Process is still running";
	}

	/**
	 * @return the entire state as an ExitStatus object
	 */
	public ExitStatus getExitStatus() {
		final ExecutorFinishStatus status = (exitedOk ? (exitValue == 0 ? ExecutorFinishStatus.NORMAL : ExecutorFinishStatus.ERROR) : ExecutorFinishStatus.TIMED_OUT);
		return new ExitStatus(-1, exitValue, status, sbOut.toString(), sbErr.toString());
	}

	/**
	 * @param command tokenized command to execute
	 * @param redirectStderr whether or not to redirect stderr to stdout
	 * @param cleanEnvironment whether or not to start from a clean environment (<code>true</code>) or inherit it from the JVM (<code>false</code>)
	 * @param timeoutValue how much time to wait for
	 * @param unit unit of time to wait, applies to timeoutValue
	 * @return the ExitStatus (stdout, stderr, process exit status - if normal - and the execution status)
	 */
	public static ExitStatus executeCommand(final List<String> command, final boolean redirectStderr, final boolean cleanEnvironment, final int timeoutValue, final TimeUnit unit) {
		final ProcessBuilder pBuilder = new ProcessBuilder(command);

		if (cleanEnvironment)
			pBuilder.environment().clear();

		pBuilder.redirectErrorStream(redirectStderr);

		try {
			final Process p = pBuilder.start();

			final ProcessWithTimeout pTimeout = new ProcessWithTimeout(p, pBuilder);
			pTimeout.waitFor(timeoutValue, unit);
			return pTimeout.getExitStatus();
		}
		catch (final IOException ioe) {
			if (logger.isLoggable(Level.WARNING))
				logger.log(Level.WARNING, "Exception executing `" + command + "` with cleanEnv=" + cleanEnvironment, ioe);

			return new ExitStatus(-1, 0, ExecutorFinishStatus.ERROR, null, null);
		}
		catch (final InterruptedException e) {
			if (logger.isLoggable(Level.WARNING))
				logger.log(Level.WARNING, "Interrupted while executing `" + command + "` with cleanEnv=" + cleanEnvironment + " and " + timeoutValue + " " + unit, e);

			return new ExitStatus(-1, 0, ExecutorFinishStatus.TIMED_OUT, null, null);
		}
	}
}
