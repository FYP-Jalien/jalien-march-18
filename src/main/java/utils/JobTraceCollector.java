/**
 *
 */
package utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import alien.config.ConfigUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;

/**
 * @author costing
 * @since Jun 22, 2021
 */
public class JobTraceCollector {
	private static final ExecutorService asyncOperations = new CachedThreadPool(16, 1, TimeUnit.MINUTES);

	private static int port = 8000;

	private static String baseLocation = ConfigUtils.getConfig().gets("utils.JobTraceCollector.basePath", "/home/alienmaster/ALICE/tmp/joblog");

	private static String targetServer = ConfigUtils.getConfig().gets("utils.JobTraceCollector.traceServer", "aliendb10.cern.ch");

	private static boolean hasTracesLocally = ConfigUtils.getConfig().getb("utils.JobTraceCollector.tracesAreLocal", false);

	private static DatagramSocket senderSocket;

	private static SocketAddress targetServerAddress;

	private static Monitor monitor = MonitorFactory.getMonitor(JobTraceCollector.class.getCanonicalName());

	static {
		try {
			senderSocket = new DatagramSocket();

			targetServerAddress = new InetSocketAddress(InetAddress.getByName(targetServer), port);
		}
		catch (final Exception e) {
			System.err.println("Exception initializing sockets: " + e.getMessage());
			System.exit(1);
		}
	}

	/**
	 * @author costing
	 * @since Jun 22, 2021
	 */
	public static final class TraceMessage implements Runnable {
		final long timestamp;
		final long jobid;
		final String action;
		final String message;

		/**
		 * @param buffer
		 * @param len
		 * @throws IOException
		 */
		public TraceMessage(final byte[] buffer, final int len) throws IOException {
			try (DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer, 0, len))) {
				timestamp = dis.readLong();
				jobid = dis.readLong();
				action = dis.readUTF();
				message = dis.readUTF();
			}
		}

		/**
		 * @param timestamp
		 * @param jobid
		 * @param action
		 * @param message
		 */
		public TraceMessage(final long timestamp, final long jobid, final String action, final String message) {
			this.timestamp = timestamp;
			this.jobid = jobid;
			this.action = action;
			this.message = message;
		}

		/**
		 * Send or write to disk
		 */
		public void send() {
			if (hasTracesLocally)
				run();
			else
				sendUDP();
		}

		private void sendUDP() {
			try (ByteArrayOutputStream baos = new ByteArrayOutputStream(action.length() + message.length() + 32); DataOutputStream dos = new DataOutputStream(baos)) {
				dos.writeLong(timestamp);
				dos.writeLong(jobid);
				dos.writeUTF(action);
				dos.writeUTF(message);

				final byte[] data = baos.toByteArray();

				final DatagramPacket packet = new DatagramPacket(data, data.length, targetServerAddress);
				senderSocket.send(packet);
			}
			catch (final IOException ioe) {
				System.err.println("Cannot send message: " + ioe.getMessage());
			}
		}

		@Override
		public void run() {
			final File f = new File(baseLocation + "/" + (jobid / 10000) + "/" + jobid + ".log");

			if (!f.exists()) {
				final File fDir = f.getParentFile();

				if (!fDir.exists() && !fDir.mkdirs()) {
					System.err.println("Cannot create " + fDir.getAbsolutePath());
					return;
				}
			}

			final String line = String.format("%d [%-10s]: %s", Long.valueOf(timestamp), action, message);

			try (PrintWriter pw = new PrintWriter(new FileWriter(f, true))) {
				pw.println(line);
			}
			catch (final IOException ioe) {
				System.err.println("Cannot write to " + f.getAbsolutePath() + " : " + ioe.getMessage());
			}
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		if (args.length > 0) {
			System.err.println("Inserting a test message");

			final TraceMessage t = new TraceMessage(System.currentTimeMillis(), 2345678901L, "proc", "arguments received from command line: " + args[0]);
			t.send();
			return;
		}

		try (DatagramSocket serverSocket = new DatagramSocket(port)) {
			final byte[] buffer = new byte[65535];

			final DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

			while (true) {
				serverSocket.receive(packet);
				final int len = packet.getLength();

				if (len < 10)
					continue;

				try {
					asyncOperations.submit(new TraceMessage(buffer, len));

					monitor.incrementCounter("receivedMessages");
				}
				catch (final IOException ioe) {
					System.err.println("Received invalid packet from " + packet.getSocketAddress() + " : " + ioe.getMessage());
					monitor.incrementCounter("invalidMessages");
				}
			}
		}
	}
}
