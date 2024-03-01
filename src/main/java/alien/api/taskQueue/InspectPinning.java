package alien.api.taskQueue;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import alien.api.Request;
import alien.taskQueue.TaskQueueUtils;
import alien.user.AliEnPrincipal;

/**
 * Inspect the pinning config of other JR running in the machine
 *
 * @author marta
 * @since
 */
public class InspectPinning extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 5022083696413315512L;

	private final boolean dbRemoval;

	private final byte[] proposedMask;

	private final String hostname;

	private final UUID vmUID;

	private byte[] responseMask;

	/**
	 * @param user
	 * @param mask to be pinned
	 * @param dbRemoval should the entry be removed from db
	 * @param hostname name of the executing machine
	 */
	public InspectPinning(final AliEnPrincipal user, final byte[] mask, final boolean dbRemoval, String hostname) {
		setRequestUser(user);
		this.proposedMask = mask;
		this.dbRemoval = dbRemoval;
		this.hostname = hostname;
		this.vmUID = this.getVMUUID();
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(String.valueOf(proposedMask), String.valueOf(dbRemoval), String.valueOf(vmUID), hostname);
	}

	@Override
	public void run() {
		responseMask = TaskQueueUtils.getPinningStatus(proposedMask, dbRemoval, vmUID, hostname);
	}

	/**
	 * @return a JDL
	 */
	public byte[] getResponseMask() {
		return this.responseMask;
	}

	@Override
	public String toString() {
		return "Asked for checking pinning status :  reply is: " + this.responseMask;
	}
}
