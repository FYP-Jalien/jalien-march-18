package alien.catalogue;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.api.catalogue.UpdateGUIDMD5;
import alien.catalogue.access.AccessType;
import alien.catalogue.access.AuthorizationFactory;
import alien.config.ConfigUtils;
import alien.io.IOUtils;
import alien.io.protocols.TempFileManager;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.user.AliEnPrincipal;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import lazyj.DBFunctions;
import lazyj.Format;
import lia.util.process.ExternalProcesses;

/**
 * @author costing
 *
 */
public final class GUIDUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(GUIDUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(GUIDUtils.class.getCanonicalName());

	/**
	 * Get the host where this entry should be located
	 *
	 * @param guid
	 * @return host id
	 * @see Host
	 */
	public static int getGUIDHost(final UUID guid) {
		final long guidTime = indexTime(guid);

		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);

		if (index == null)
			return -1;

		return index.hostIndex;
	}

	/**
	 * Get the DB connection that applies for a particular GUID
	 *
	 * @param guid
	 * @return the DB connection, or <code>null</code> if something is not right
	 * @see #getTableNameForGUID(UUID)
	 */
	public static DBFunctions getDBForGUID(final UUID guid) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		return h.getDB();
	}

	/**
	 * Get the tablename where this GUID should be located (if any)
	 *
	 * @param guid
	 * @return table name, or <code>null</code> if any problem
	 * @see #getDBForGUID(UUID)
	 */
	public static int getTableNameForGUID(final UUID guid) {
		final long guidTime = indexTime(guid);

		final GUIDIndex index = CatalogueUtils.getGUIDIndex(guidTime);

		if (index == null)
			return -1;

		return index.tableName;
	}

	/**
	 * @param l
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final LFN l) {
		return getGUID(l, false);
	}

	/**
	 * @param l
	 * @param evenIfDoesntExist
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final LFN l, final boolean evenIfDoesntExist) {
		if (l.guid == null)
			return null;

		final GUID g = getGUID(l.guid, evenIfDoesntExist);

		if (g == null)
			return null;

		g.addKnownLFN(l);

		return g;
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 *
	 * @param uuid
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final String uuid) {
		return getGUID(UUID.fromString(uuid));
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 *
	 * @param guid
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final UUID guid) {
		return getGUID(guid, false);
	}

	/**
	 * Get the referring GUIDs (members of the archive, if any)
	 *
	 * @param guid
	 * @return the set of GUIDs pointing to this archive, or <code>null</code> if there is no such file
	 */
	public static Set<GUID> getReferringGUID(final UUID guid) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		try (DBFunctions db = h.getDB()) {
			if (db == null)
				return null;

			db.setReadOnly(true);

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				return null;

			if (monitor != null)
				monitor.incrementCounter("GUID_db_lookup");

			if (!db.query("select G" + tableName + "L.* from G" + tableName + "L INNER JOIN G" + tableName + "L_PFN USING (guidId) where pfn like ?;", false, "guid:///" + guid.toString() + "?ZIP=%"))
				throw new IllegalStateException("Failed querying the G" + tableName + "L table for guid " + guid);

			if (!db.moveNext())
				return null;

			final Set<GUID> ret = new TreeSet<>();

			do
				try {
					ret.add(new GUID(db, host, tableName));
				}
				catch (final Exception e) {
					logger.log(Level.WARNING, "Exception instantiating guid " + guid + " from " + tableName, e);

					return null;
				}
			while (db.moveNext());

			return ret;
		}
	}

	/**
	 * Bulk operation to retrieve GUID objects from the catalogue
	 *
	 * @param guidList
	 *            List of UUIDs to retrieve the GUIDs for
	 * @return the set of GUIDs that could be looked up in the catalogue
	 */
	public static Set<GUID> getGUIDs(final UUID... guidList) {
		final Set<GUID> ret = new LinkedHashSet<>();

		if (guidList == null || guidList.length == 0)
			return ret;

		final Map<Host, Map<Integer, Set<UUID>>> mapping = new LinkedHashMap<>();

		for (final UUID guid : guidList) {
			final int host = getGUIDHost(guid);

			if (host < 0)
				continue;

			final Host h = CatalogueUtils.getHost(host);

			if (h == null)
				continue;

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				continue;

			final Integer iTableName = Integer.valueOf(tableName);

			Map<Integer, Set<UUID>> hostMap = mapping.get(h);

			if (hostMap == null) {
				hostMap = new LinkedHashMap<>();
				mapping.put(h, hostMap);
			}

			Set<UUID> uuidList = hostMap.get(iTableName);

			if (uuidList == null) {
				uuidList = new LinkedHashSet<>();
				hostMap.put(iTableName, uuidList);
			}

			uuidList.add(guid);
		}

		for (final Map.Entry<Host, Map<Integer, Set<UUID>>> entry : mapping.entrySet()) {
			final Host h = entry.getKey();

			final Map<Integer, Set<UUID>> hostMapping = entry.getValue();

			try (DBFunctions db = h.getDB()) {
				if (db == null)
					continue;

				db.setReadOnly(true);
				db.setQueryTimeout(600); // in normal conditions it cannot take 10 minutes to ask for up to 100 guids from a table

				for (final Map.Entry<Integer, Set<UUID>> tableEntry : hostMapping.entrySet()) {
					final Integer tableName = tableEntry.getKey();

					if (monitor != null)
						monitor.incrementCounter("GUID_db_lookup");

					final StringBuilder sb = new StringBuilder();

					final ArrayList<UUID> allGUIDs = new ArrayList<>(tableEntry.getValue());

					for (int i = 0; i < allGUIDs.size(); i += IndexTableEntry.MAX_QUERY_LENGTH) {
						sb.setLength(0);
						final List<UUID> sublist = allGUIDs.subList(i, Math.min(i + IndexTableEntry.MAX_QUERY_LENGTH, allGUIDs.size()));

						for (final UUID u : sublist) {
							if (sb.length() > 0)
								sb.append(',');

							sb.append("string2binary('").append(u.toString()).append("')");
						}

						final String q = "SELECT SQL_NO_CACHE * FROM G" + tableName + "L WHERE guid IN (" + sb.toString() + ");";

						if (!db.query(q))
							throw new IllegalStateException("Failed executing query: " + q);

						while (db.moveNext())
							try {
								ret.add(new GUID(db, h.hostIndex, tableName.intValue()));
							}
							catch (final Exception e) {
								logger.log(Level.WARNING, "Exception instantiating some guid from " + tableName, e);
							}
					}
				}
			}
		}

		return ret;
	}

	/**
	 * Get the GUID catalogue entry when the uuid is known
	 *
	 * @param guid
	 * @param evenIfDoesntExist
	 *            if <code>true</code>, if the entry doesn't exist then a new GUID is returned
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final UUID guid, final boolean evenIfDoesntExist) {
		final int host = getGUIDHost(guid);

		if (host < 0)
			return null;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return null;

		try (DBFunctions db = h.getDB()) {
			if (db == null)
				return null;

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				return null;

			if (monitor != null)
				monitor.incrementCounter("GUID_db_lookup");

			db.setReadOnly(true);

			db.setQueryTimeout(600);

			if (!db.query("SELECT * FROM G" + tableName + "L WHERE guid=string2binary(?);", false, guid.toString()))
				throw new IllegalStateException("Failed querying the G" + tableName + "L table for guid " + guid);

			if (!db.moveNext()) {
				if (evenIfDoesntExist)
					return new GUID(guid);

				return null;
			}

			try {
				return new GUID(db, host, tableName);
			}
			catch (final Exception e) {
				logger.log(Level.WARNING, "Exception instantiating guid " + guid + " from " + tableName, e);

				return null;
			}
		}
	}

	/**
	 *
	 * check if the string contains a valid GUID
	 *
	 * @param guid
	 * @return yesORno
	 */
	public static boolean isValidGUID(final String guid) {
		try {
			UUID.fromString(guid);
			return true;
		}
		catch (@SuppressWarnings("unused") final Exception e) {
			return false;
		}
	}

	private static int clockSequence = MonitorFactory.getSelfProcessID();

	private static long lastTimestamp = System.nanoTime() / 100 + 122192928000000000L;

	private static long lastTimestamp2 = System.nanoTime() / 100 + 122192928000000000L;

	/**
	 * @return a time UUID with the reference time set to now
	 */
	public static synchronized UUID generateTimeUUID() {
		final long time = System.currentTimeMillis() * 10000 + 122192928000000000L;

		if (time <= lastTimestamp) {
			clockSequence++;

			if (clockSequence >= 65535)
				clockSequence = 0;
		}

		lastTimestamp = time;

		return generateTimeUUIDWork(time);
	}

	/**
	 * @param referenceTime
	 * @return a time UUID with the time field set to the reference time
	 */
	public static synchronized UUID generateTimeUUID(final long referenceTime) {
		final long time = referenceTime * 10000 + 122192928000000000L;

		if (time <= lastTimestamp2 || time <= lastTimestamp) {
			clockSequence++;

			if (clockSequence >= 65535)
				clockSequence = 0;
		}

		lastTimestamp2 = time;

		return generateTimeUUIDWork(time);
	}

	/**
	 * @return a new time-based (version 1) UUID
	 */
	private static UUID generateTimeUUIDWork(final long time) {
		final byte[] contents = new byte[16];

		final byte[] mac = getMac();

		for (int i = 0; i < 6; i++)
			contents[10 + i] = mac[i];

		final int timeHi = (int) (time >>> 32);
		final int timeLo = (int) time;

		contents[0] = (byte) (timeLo >>> 24);
		contents[1] = (byte) (timeLo >>> 16);
		contents[2] = (byte) (timeLo >>> 8);
		contents[3] = (byte) (timeLo);

		contents[4] = (byte) (timeHi >>> 8);
		contents[5] = (byte) timeHi;
		contents[6] = (byte) (timeHi >>> 24);
		contents[7] = (byte) (timeHi >>> 16);

		contents[8] = (byte) (clockSequence >> 8);
		contents[9] = (byte) clockSequence;

		contents[6] &= (byte) 0x0F;
		contents[6] |= (byte) 0x10;

		contents[8] &= (byte) 0x3F;
		contents[8] |= (byte) 0x80;

		final UUID ret = GUID.getUUID(contents);

		return ret;
	}

	/**
	 * Extract the MAC address from the given UUID. There is no guarantee on the value of this field, it's just the bytes that would have the MAC address in a v1 UUID.
	 *
	 * @param uuid
	 * @return MAC address of the machine that generated the UUID (not guaranteed information)
	 */
	public static String getMacAddr(final UUID uuid) {
		final long mac = uuid.getLeastSignificantBits() & 0xFFFFFFFFFFFFL;

		return String.format("%02x:%02x:%02x:%02x:%02x:%02x", Long.valueOf(mac >> 40 & 0xFF), Long.valueOf(mac >> 32 & 0xFF), Long.valueOf(mac >> 24 & 0xFF), Long.valueOf(mac >> 16 & 0xFF),
				Long.valueOf(mac >> 8 & 0xFF), Long.valueOf(mac & 0xFF));
	}

	private static byte[] MACAddress = null;

	private static final String SYS_ENTRY = "/sys/class/net";

	/**
	 * @return One of the MAC addresses of the machine where this code is running
	 */
	public static synchronized byte[] getMac() {
		if (MACAddress == null) {
			// figure it out
			MACAddress = new byte[6];

			String sMac = null;

			final File f = new File(SYS_ENTRY);

			if (f.exists()) {
				final String[] devices = f.list();

				if (devices != null) {
					final List<String> files = Arrays.asList(devices);

					Collections.sort(files);

					for (final String dev : devices) {
						final String addr = lazyj.Utils.readFile(SYS_ENTRY + "/" + dev + "/address");

						if (addr != null && !addr.equals("00:00:00:00:00:00")) {
							sMac = addr;
							break;
						}
					}
				}
			}

			if (sMac == null)
				try (BufferedReader br = new BufferedReader(new StringReader(ExternalProcesses.getCmdOutput(Arrays.asList("/sbin/ifconfig", "-a"), false, 30, TimeUnit.SECONDS)))) {
					String s;

					while ((s = br.readLine()) != null) {
						final StringTokenizer st = new StringTokenizer(s);

						while (st.hasMoreTokens()) {
							final String tok = st.nextToken();

							if (tok.equals("HWaddr") && st.hasMoreTokens()) {
								sMac = st.nextToken();
								break;
							}
						}

						if (sMac != null)
							break;
					}
				}
				catch (@SuppressWarnings("unused") final Throwable t) {
					// ignore
				}

			if (sMac != null) {
				final StringTokenizer st = new StringTokenizer(sMac.trim(), ":");

				for (int i = 0; i < 6; i++)
					try {
						MACAddress[i] = (byte) Integer.parseInt(st.nextToken(), 16);
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						// ignore
					}
			}
		}

		return MACAddress;
	}

	/**
	 * @return a new (empty) GUID
	 */
	public static GUID createGuid() {
		UUID id;
		// do{
		// id = generateTimeUUID();
		// } while (getGUID(id) != null);

		id = generateTimeUUID();

		return new GUID(id);
	}

	/**
	 * @param user
	 * @return a new GUID
	 */
	public static GUID createGuid(final AliEnPrincipal user) {
		final GUID guid = createGuid();

		if (user != null) {
			guid.owner = user.getName();

			final Set<String> roles = user.getRoles();

			if (roles != null && roles.size() > 0)
				guid.gowner = roles.iterator().next();
			else
				guid.gowner = guid.owner;
		}

		guid.type = 0; // as in the catalogue
		guid.perm = "755";
		guid.aclId = -1;

		return guid;
	}

	/**
	 * @param f
	 *            base file to fill the properties from: ctime, md5, sizeSystem.
	 * @param user
	 *            who owns this new entry
	 * @return the newly created GUID
	 * @throws IOException
	 */
	public static GUID createGuid(final File f, final AliEnPrincipal user) throws IOException {
		final String md5 = IOUtils.getMD5(f);

		final GUID guid = createGuid(user);

		guid.ctime = new Date(f.lastModified());
		guid.md5 = md5;
		guid.size = f.length();

		return guid;
	}

	/**
	 * @param uuid
	 * @return epoch time of this uuid
	 */
	public static final long epochTime(final UUID uuid) {
		return (uuid.timestamp() - 0x01b21dd213814000L) / 10000;
	}

	/**
	 * @param uuid
	 * @return AliEn guidtime-compatible value
	 */
	public static final long indexTime(final UUID uuid) {
		final long msg = uuid.getMostSignificantBits() & 0x00000000FFFFFFFFL;

		long ret = (msg >>> 16);
		ret += (msg & 0x0FFFFL) << 16;

		return ret;
	}

	/**
	 * @param uuid
	 * @return index time as string
	 */
	public static final String getIndexTime(final UUID uuid) {
		return Long.toHexString(indexTime(uuid)).toUpperCase();
	}

	/**
	 * Check if the MD5 sum is set to both the LFN and the underlying GUID. If not set the missing one (or both) from the other or by downloading the file and computing the MD5 sum.
	 *
	 * @param lfn
	 * @return <code>true</code> if the MD5 was already set or if it could be now set, <code>false</code> if there was any error setting it
	 */
	public static boolean checkMD5(final LFN lfn) {
		final GUID g = getGUID(lfn);

		if (g == null) {
			logger.log(Level.WARNING, "No GUID for " + lfn.getCanonicalName());
			return false;
		}

		if (lfn.md5 == null || lfn.md5.length() < 10 || g.md5 == null || g.md5.length() < 10) {
			if (g.md5 != null && g.md5.length() >= 10) {
				lfn.md5 = g.md5;
				logger.log(Level.INFO, "Setting md5 of " + g.guid + " from " + lfn.getCanonicalName() + " to " + lfn.md5);
				return lfn.update();
			}

			if (lfn.md5 != null && lfn.md5.length() >= 10) {
				g.md5 = lfn.md5;
				logger.log(Level.INFO, "Setting md5 of " + lfn.getCanonicalName() + " from " + g.guid + " to " + g.md5);
				return g.update();
			}

			final String reason = AuthorizationFactory.fillAccess(g, AccessType.READ);

			if (reason != null) {
				logger.log(Level.WARNING, "Could not get authorization to read " + g.guid + " : " + reason);
				return false;
			}

			final File temp = IOUtils.get(g);

			if (temp != null)
				try {
					g.md5 = IOUtils.getMD5(temp);

					if (!g.update())
						return false;

					lfn.md5 = g.md5;

					if (!lfn.update())
						return false;

					return true;
				}
				catch (final IOException ioe) {
					logger.log(Level.WARNING, "Unable to compute the MD5 sum of " + lfn.getCanonicalName(), ioe);

					return false;
				}
				finally {
					TempFileManager.release(temp);
					temp.delete();
				}

			logger.log(Level.WARNING, "Could not download " + g.guid);

			return false;
		}

		return true;
	}

	/**
	 * @param guid
	 * @param md5
	 * @return <code>true</code> if the MD5 was already set or if it could be now set, <code>false</code> if there was any error setting it
	 */
	public static boolean updateMd5(UUID guid, String md5) {

		if (guid == null || md5 == null)
			return false;

		if (!ConfigUtils.isCentralService())
			try {
				final UpdateGUIDMD5 request = new UpdateGUIDMD5(null, guid, md5);
				return Dispatcher.execute(request).isUpdateSuccessful();
			}
			catch (@SuppressWarnings("unused") final ServerException se) {
				return false;
			}

		final int host = getGUIDHost(guid);

		if (host < 0)
			return false;

		final Host h = CatalogueUtils.getHost(host);

		if (h == null)
			return false;

		try (DBFunctions db = h.getDB()) {
			if (db == null)
				return false;

			final int tableName = GUIDUtils.getTableNameForGUID(guid);

			if (tableName < 0)
				return false;

			if (!db.query("UPDATE G" + tableName + "L SET md5='" + Format.escSQL(md5) + "' WHERE guid=string2binary('" + Format.escSQL(guid.toString()) + "') AND (md5 is null OR length(md5) = 0)"))
				return false;

			if (db.getUpdateCount() == 0)
				return false;

			List<LFN> lfns = LFNUtils.getLFNsFromUUIDs(Set.of(guid));

			for(LFN lfn : lfns) {
				String guidMD5 = GUIDUtils.getGUID(lfn).getMD5();
				if (guidMD5 != null && guidMD5.length() > 0)
					GUIDUtils.checkMD5(lfn);
			}
		}

		return true;
	}

	/**
	 * @param pfn
	 * @return the GUID, or <code>null</code> if it cannot be located
	 */
	public static GUID getGUID(final PFN pfn) {
		return new GUID(pfn);
	}
}
