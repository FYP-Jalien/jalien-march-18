package alien.api;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;

/**
 * @author costing
 * @since 2011-03-04
 */
public abstract class Request implements Serializable, Runnable {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(Request.class.getCanonicalName());

	/**
	 *
	 */
	private static final long serialVersionUID = -8044096743871226167L;

	/**
	 * Unique identifier of the VM, for communication purposes
	 */
	private static final UUID VM_UUID = UUID.randomUUID();

	/**
	 * Sequence number, for dispatching asynchronous messages
	 */
	private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

	/**
	 * @return this VM's unique identifier
	 */
	public static final UUID getVMID() {
		return VM_UUID;
	}

	/**
	 * @return current sequence number (number of requests created since VM startup)
	 */
	public static final Long getCurrentSequenceNumber() {
		return Long.valueOf(ID_SEQUENCE.get());
	}

	/**
	 * Unique identifier of the VM that made the request
	 */
	private final UUID vm_uuid = VM_UUID;

	/**
	 * Request ID in the VM
	 */
	private final Long requestID = Long.valueOf(ID_SEQUENCE.incrementAndGet());

	/**
	 * The default identity of the VM
	 */
	private final AliEnPrincipal requester_uid = AuthorizationFactory.getDefaultUser();

	/**
	 * Effective identity (the user on behalf of whom the request came)
	 */
	private AliEnPrincipal requester_euid = requester_uid;

	/**
	 * Requested identity (the user on behalf of whom the request should be executed)
	 */
	private AliEnPrincipal requester_ruid = requester_uid;

	/**
	 * Set on receiving a request over the network
	 */
	private transient AliEnPrincipal partner_identity = null;

	/**
	 * Set on receiving a request over the network
	 */
	private transient X509Certificate[] partner_certificate = null;

	/**
	 * Set on receiving a request over the network
	 */
	private transient InetAddress partner_address = null;

	/**
	 * @return the unique identifier of the VM that generated the request
	 */
	public final UUID getVMUUID() {
		return vm_uuid;
	}

	/**
	 * @return sequence number within the VM that generated the request
	 */
	public final Long getRequestID() {
		return requestID;
	}

	/**
	 * @return requester identity (default identity of the VM)
	 */
	public final AliEnPrincipal getRequesterIdentity() {
		return requester_uid;
	}

	/**
	 * @return effective user on behalf of whom the request is executed
	 */
	public final AliEnPrincipal getEffectiveRequester() {
		return requester_euid;
	}

	/**
	 * @return identity of the partner, set on receiving a request over the wire
	 */
	public final AliEnPrincipal getPartnerIdentity() {
		return partner_identity;
	}

	/**
	 * @return certificate of the partner, set on receiving a request over the wire
	 */
	public final X509Certificate[] getPartnerCertificate() {
		if (partner_certificate == null)
			return null;

		return Arrays.copyOf(partner_certificate, partner_certificate.length);
	}

	/**
	 * @param id
	 *            identity of the partner. This is called on receiving a request over the wire.
	 */
	protected final void setPartnerIdentity(final AliEnPrincipal id) {
		if (partner_identity != null)
			throw new IllegalAccessError("You are not allowed to overwrite this field!");

		partner_identity = id;
	}

	/**
	 * @param cert
	 *            certificate of the partner. This is called on receiving a request over the wire.
	 */
	protected final void setPartnerCertificate(final X509Certificate[] cert) {
		if (partner_certificate != null)
			throw new IllegalAccessError("You are not allowed to overwrite this field!");

		partner_certificate = cert;
	}

	/**
	 * let the request run with a different user name
	 *
	 * @param user
	 */
	protected final void setRequestUser(final AliEnPrincipal user) {
		requester_ruid = user;
	}

	/**
	 * Authorize a role change
	 *
	 * @return permission for role change
	 */
	protected final boolean authorizeUserAndRole() {

		// System.err.println("uid: "+requester_uid);
		// System.err.println("ruid: "+requester_ruid);
		// System.err.println("rrid: "+requester_rrid);

		// first the user
		if (requester_uid != null)
			if (requester_ruid != null) {
				if ((requester_ruid.getName() != null) && requester_uid.canBecome(requester_ruid.getName())) {
					if (logger.isLoggable(Level.FINE))
						logger.log(Level.FINE, "Successfully switched user from '" + requester_euid + "' to '" + requester_ruid + "'.");

					requester_euid = requester_ruid;

					return true;
				}
			}
			else {
				if (logger.isLoggable(Level.FINE))
					logger.log(Level.FINE, "User '" + requester_uid + "' doesn't indicate a different role, keeping itself by default");

				requester_euid = requester_ruid = requester_uid;
				return true;
			}

		logger.log(Level.WARNING, "User '" + requester_uid + "' was denied role switching action to '" + requester_ruid + "'.");

		return false;
	}

	/**
	 * @return partner's IP address
	 */
	public InetAddress getPartnerAddress() {
		return partner_address;
	}

	/**
	 * @param ip
	 *            partner's address
	 */
	public final void setPartnerAddress(final InetAddress ip) {
		if (this.partner_address != null && !this.partner_address.equals(ip))
			throw new IllegalAccessError("You are not allowed to overwrite this field from " + this.partner_address + " to " + ip);

		this.partner_address = ip;
	}

	private ServerException exception = null;

	/**
	 * In case of an execution problem set the exception flag to the underlying cause
	 *
	 * @param exception
	 */
	public final void setException(final ServerException exception) {
		this.exception = exception;
	}

	/**
	 * @return the server side exception, if any
	 */
	public final ServerException getException() {
		return exception;
	}

	/**
	 * Custom deserialization, making sure the transient fields are not set
	 *
	 * @param stream
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(final java.io.ObjectInputStream stream) throws IOException, ClassNotFoundException {
		stream.defaultReadObject();

		this.partner_address = null;
		this.partner_identity = null;
		this.partner_certificate = null;
	}

	/**
	 * @return the arguments to this object, for logging purposes.
	 *         Please override this method and return any parameter that is relevant to build the object. To be used in activity logging.
	 */
	public abstract List<String> getArguments();
}
