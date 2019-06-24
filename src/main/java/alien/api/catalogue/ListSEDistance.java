package alien.api.catalogue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import alien.api.Request;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.catalogue.PFN;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the LFN object for this path
 *
 * @author costing
 * @since 2011-03-04
 */
public class ListSEDistance extends Request {
	private static final long serialVersionUID = 726995834931008148L;
	private final String lfn_name;
	private final boolean write;
	private String site;
	private List<SE> ses;
	private List<HashMap<SE, Double>> distances;
	private final String qos;

	/**
	 * @param user
	 * @param sitename
	 * @param write
	 * @param lfn_name
	 * @param qos
	 *            QoS to restrict to, if not <code>null</code>
	 */
	public ListSEDistance(final AliEnPrincipal user, final String sitename, final boolean write, final String lfn_name, final String qos) {
		setRequestUser(user);
		this.lfn_name = lfn_name;
		this.write = write;
		this.qos = qos;

		if (sitename == null || sitename.length() == 0)
			this.site = ConfigUtils.getCloseSite();
		else
			this.site = sitename;
	}

	@Override
	public void run() {
		// this is for write
		if (this.write)
			this.ses = SEUtils.getClosestSEs(this.site, true);
		else {
			// for read with lfn specified

			if (this.lfn_name == null || this.lfn_name.length() == 0)
				this.ses = SEUtils.getClosestSEs(this.site, false);
			else {
				this.ses = new ArrayList<>();

				LFN lfn = null;
				if (this.lfn_name != null && this.lfn_name.length() != 0)
					lfn = LFNUtils.getLFN(this.lfn_name);

				if (lfn == null)
					return;

				final List<PFN> lp = SEUtils.sortBySite(lfn.whereis(), this.site, true, false);

				if (lp == null)
					return;

				for (final PFN p : lp)
					this.ses.add(p.getSE());
			}
		}

		this.distances = new LinkedList<>();

		for (final SE se : this.ses) {
			if (qos != null && !se.isQosType(qos))
				continue;

			final HashMap<SE, Double> hm = new HashMap<>();
			hm.put(se, SEUtils.getDistance(this.site, se, write));
			this.distances.add(hm);
		}
	}

	/**
	 * @return SE list sorted by distance
	 */
	public List<SE> getSE() {
		return this.ses;
	}

	/**
	 * @return distance list
	 */
	public List<HashMap<SE, Double>> getSEDistances() {
		return this.distances;
	}
}
