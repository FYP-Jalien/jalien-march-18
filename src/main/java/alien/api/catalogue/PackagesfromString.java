package alien.api.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import alien.api.Request;
import alien.catalogue.Package;
import alien.catalogue.PackageUtils;
import alien.user.AliEnPrincipal;

/**
 * Get the packages
 *
 * @author ron
 * @since Nov 23, 2011
 */
public class PackagesfromString extends Request {

	/**
	 *
	 */
	private static final long serialVersionUID = -9135907539296101201L;

	private final String platform;

	private List<Package> packages;

	/**
	 * @param user
	 * @param platform
	 */
	public PackagesfromString(final AliEnPrincipal user, final String platform) {
		setRequestUser(user);
		this.platform = platform;
	}

	@Override
	public List<String> getArguments() {
		return Arrays.asList(this.platform);
	}

	@Override
	public void run() {
		final List<Package> all = PackageUtils.getPackages();

		if (platform == null || platform.equals("all")) {
			this.packages = all;
			return;
		}

		this.packages = new ArrayList<>(all.size());

		for (final Package p : all)
			if (p.isAvailable(platform) || p.isAvailable("source"))
				this.packages.add(p);
	}

	/**
	 * @return the requested LFN
	 */
	public List<Package> getPackages() {
		return this.packages;
	}

	@Override
	public String toString() {
		return "Asked for : " + this.platform + ", reply is: " + this.packages;
	}
}
