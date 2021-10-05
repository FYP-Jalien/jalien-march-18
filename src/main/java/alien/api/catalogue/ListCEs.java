package alien.api.catalogue;

import java.util.ArrayList;
import java.util.List;

import alien.api.Request;
import alien.api.taskQueue.CE;
import alien.config.ConfigUtils;
import alien.user.AliEnPrincipal;
import lazyj.DBFunctions;

/**
 * List the CEs with the given names (or all if the list is empty)
 *
 * @author marta
 */
public class ListCEs extends Request {
	/**
	 *
	 */
	private static final long serialVersionUID = 4476411896853649872L;
	private final List<String> requestedCEs;
	private List<CE> ces = new ArrayList<CE>();

	/**
	 * @param user
	 * @param requestedSEs
	 */
	public ListCEs(final AliEnPrincipal user, final List<String> requestedCEs) {
		setRequestUser(user);
		this.requestedCEs = requestedCEs;
	}

	@Override
	public List<String> getArguments() {
		return requestedCEs;
	}

	/**
	 * @return CE list
	 */
	public List<CE> getCEs() {
		return this.ces;
	}

	@Override
	public void run() {

		try (DBFunctions db = ConfigUtils.getDB("processes")) {
			if (db != null) {
				db.query("SELECT site, maxrunning, maxqueued, blocked FROM SITEQUEUES;", false);

				while (db.moveNext()) {
					if (requestedCEs != null && requestedCEs.size() > 0) {
						for (String ceName : requestedCEs) {
							if (db.gets("site").toUpperCase().contains(ceName.toUpperCase())) {
								addCEToList(db.gets("site"), db.geti("maxrunning"), db.geti("maxqueued"), db.gets("blocked"));
								continue;
							}
						}
					}
					else
						addCEToList(db.gets("site"), db.geti("maxrunning"), db.geti("maxqueued"), db.gets("blocked"));
				}
			}
		}
	}

	private void addCEToList(String site, int maxrunning, int maxqueued, String status) {
		CE ceEntry = new CE(site, maxrunning, maxqueued, status);
		this.ces.add(ceEntry);
	}
}
