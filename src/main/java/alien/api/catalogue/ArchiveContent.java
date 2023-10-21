package alien.api.catalogue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import alien.api.Request;
import alien.catalogue.GUID;
import alien.catalogue.GUIDUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the available tags associated to a path
 *
 * @author costing
 * @since 2023-10-21
 */
public class ArchiveContent extends Request {
	private static final long serialVersionUID = 322992661938966482L;

	private final Set<UUID> uuids;
	private Map<UUID, Map<GUID, String>> contents;

	/**
	 * @param user
	 * @param uuids
	 */
	public ArchiveContent(final AliEnPrincipal user, final Set<UUID> uuids) {
		setRequestUser(user);
		this.uuids = uuids;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.uuids.toString());
	}

	@Override
	public void run() {
		this.contents = new HashMap<>();

		for (final UUID uuid : this.uuids) {
			final Map<GUID, String> members = GUIDUtils.getReferringGUID(uuid);

			contents.put(uuid, members);
		}
	}

	/**
	 * @return the members of each of the UUIDs passed as arguments
	 */
	public Map<UUID, Map<GUID, String>> getContents() {
		return this.contents;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.uuids + ", reply is: " + this.contents;
	}
}
