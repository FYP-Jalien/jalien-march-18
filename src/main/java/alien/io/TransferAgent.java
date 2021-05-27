/**
 *
 */
package alien.io;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.config.ConfigUtils;
import alien.monitoring.MonitorFactory;
import lazyj.ExtProperties;

/**
 * @author costing
 * @since Dec 8, 2010
 */
public class TransferAgent extends Thread {
	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(TransferAgent.class.getCanonicalName());

	private final Integer transferAgentID;

	private final int pid = MonitorFactory.getSelfProcessID();

	private final String hostname = ConfigUtils.getLocalHostname();

	/**
	 *
	 */
	/**
	 * @param transferAgentID
	 *            unique identifier
	 */
	public TransferAgent(final int transferAgentID) {
		super("TransferAgent " + transferAgentID);

		this.transferAgentID = Integer.valueOf(transferAgentID);

		setDaemon(false);
	}

	/**
	 * @return this guy's ID
	 */
	Integer getTransferAgentID() {
		return transferAgentID;
	}

	/**
	 * @return process ID of this JVM, for activity logging purposes
	 */
	int getPID() {
		return pid;
	}

	/**
	 * @return hostname of the machine running this JVM, for activity logging purposes
	 */
	String getHostName() {
		return hostname;
	}

	private volatile Transfer work = null;

	private boolean shouldStop = false;

	private void signalStop() {
		shouldStop = true;
	}

	private static final Object moreWorkNotification = new Object();

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		try {
			final TransferBroker broker = TransferBroker.getInstance();

			boolean firstTimeNoWork = true;

			while (!shouldStop) {
				work = broker.getWork(this);

				if (work != null) {
					if (!TransferBroker.touch(work, this))
						return;

					// hey, there is work to be done, wake up _one_ neighbour
					synchronized (moreWorkNotification) {
						moreWorkNotification.notify();
					}

					logger.log(Level.INFO, "Performing transfer " + work.getTransferId());

					try {
						work.run();
					}
					catch (final Exception e) {
						logger.log(Level.WARNING, "Transfer threw exception", e);
					}
					finally {
						logger.log(Level.INFO, "Transfer finished: " + work);

						TransferBroker.notifyTransferComplete(work);

						work = null;

						TransferBroker.touch(null, this);
					}

					firstTimeNoWork = true;
				}
				else
					try {
						if (firstTimeNoWork) {
							logger.log(Level.INFO, "Agent " + transferAgentID + " : no work for me");
							firstTimeNoWork = false;
						}

						synchronized (moreWorkNotification) {
							// try in 30 seconds again to see if there is anything for it to do
							// another thread picking up work might wake us up in the mean time
							moreWorkNotification.wait(1000 * 30);
						}
					}
					catch (@SuppressWarnings("unused") final InterruptedException ie) {
						// ignore
					}
			}
		}
		catch (final Exception e) {
			logger.log(Level.SEVERE, "Exiting after an exception", e);
		}
	}

	private static int transferAgentIDSequence = 0;

	/**
	 * Run the TransferAgent<br>
	 * <br>
	 * Configuration options:<br>
	 * alien.io.TransferAgent.workers = 5 (default)
	 *
	 * @param args
	 */
	public static void main(final String args[]) {
		final ExtProperties config = alien.config.ConfigUtils.getConfig();

		int workers = config.geti("alien.io.TransferAgent.workers", 5);

		if (workers < 0 || workers > 1000) // typo ?!
			workers = 5;

		logger.log(Level.INFO, "Starting " + workers + " workers");

		final LinkedList<TransferAgent> agents = new LinkedList<>();

		for (int i = 0; i < workers; i++) {
			final TransferAgent ta = new TransferAgent(transferAgentIDSequence++);

			ta.start();

			agents.add(ta);
		}

		while (true) {
			try {
				Thread.sleep(1000 * 30);

				workers = config.geti("alien.io.TransferAgent.workers", workers);

				if (workers < 0 || workers > 1000) // typo ?!
					workers = 5;

				final Iterator<TransferAgent> it = agents.iterator();

				while (it.hasNext()) {
					final TransferAgent agent = it.next();
					if (!agent.isAlive()) {
						logger.log(Level.SEVERE, "One worker is no longer alive, removing the respective agent from the list: " + agent.getName());

						it.remove();
					}
				}

				while (workers > agents.size()) {
					final TransferAgent ta = new TransferAgent(transferAgentIDSequence++);

					ta.start();

					agents.add(ta);
				}

				while (agents.size() > workers) {
					final TransferAgent ta = agents.removeLast();

					ta.signalStop();
				}
			}
			catch (@SuppressWarnings("unused") final Exception e) {
				// ignore
			}

			for (final TransferAgent ta : agents)
				TransferBroker.touch(ta.work, ta);
		}
	}

}
