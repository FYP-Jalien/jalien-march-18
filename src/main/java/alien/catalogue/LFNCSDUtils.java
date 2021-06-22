package alien.catalogue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.nfunk.jep.JEP;

import com.datastax.driver.core.ConsistencyLevel;

import alien.catalogue.recursive.Append;
import alien.catalogue.recursive.Chown;
import alien.catalogue.recursive.Delete;
import alien.catalogue.recursive.Move;
import alien.catalogue.recursive.RecursiveOp;
import alien.config.ConfigUtils;
import alien.io.TransferUtils;
import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import alien.se.SE;
import alien.se.SEUtils;
import alien.user.AliEnPrincipal;
import alien.user.AuthorizationChecker;
import lazyj.Format;

/**
 * LFNCSD utilities
 *
 * @author mmmartin
 *
 */
public class LFNCSDUtils {

	/**
	 * Logger
	 */
	static final Logger logger = ConfigUtils.getLogger(LFNCSDUtils.class.getCanonicalName());

	/**
	 * Monitoring component
	 */
	static final Monitor monitor = MonitorFactory.getMonitor(LFNCSDUtils.class.getCanonicalName());

	/** Thread pool */
	static ThreadPoolExecutor tPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

	static {
		tPool.setKeepAliveTime(1, TimeUnit.MINUTES);
		tPool.allowCoreThreadTimeOut(true);
	}

	/**
	 * Cassandra table suffix
	 */
	public static final String append_table = "";
	/**
	 * Cassandra consistency
	 */
	public static final ConsistencyLevel clevel = ConsistencyLevel.QUORUM;

	/**
	 * the "-s" flag of AliEn `find`
	 */
	public static final int FIND_NO_SORT = 1;

	/**
	 * the "-d" flag of AliEn `find`
	 */
	public static final int FIND_INCLUDE_DIRS = 2;

	/**
	 * the "-y" flag of AliEn `find`
	 */
	public static final int FIND_BIGGEST_VERSION = 4;

	/**
	 * @param operation
	 * @param start_path
	 * @param pattern
	 * @param metadata
	 * @param flags
	 * @return list of lfns that fit the patterns, if any
	 */
	public static boolean recurseAndFilterLFNs(final RecursiveOp operation, final String start_path, final String pattern, final String metadata, final int flags) {
		// we create a base for search and a file pattern
		int index = 0;
		String path = start_path;
		String file_pattern = (pattern == null ? "*" : pattern);
		if (!start_path.endsWith("/") && pattern == null) {
			file_pattern = start_path.substring(start_path.lastIndexOf('/') + 1);
			path = start_path.substring(0, start_path.lastIndexOf('/') + 1);
		}

		// Split the base into directories, change asterisk and interrogation marks to regex format
		ArrayList<String> path_parts;
		if (!path.endsWith("/"))
			path += "*/";

		path = Format.replace(Format.replace(path, "*", ".*"), "?", ".");

		path_parts = new ArrayList<>(Arrays.asList(path.split("/")));

		if (file_pattern.contains("/")) {
			file_pattern = "*" + file_pattern;
		}
		file_pattern = Format.replace(Format.replace(file_pattern, "*", ".*"), "?", ".?");

		path = "/";
		path_parts.remove(0); // position 0 otherwise is an empty string
		for (final String s : path_parts) {
			if (s.contains(".*") || s.contains(".?"))
				break;
			path += s + "/";
			index++;
		}

		final Pattern pat = Pattern.compile(file_pattern);

		logger.info("Going to recurseAndFilterLFNs: " + path + " - " + file_pattern + " - " + index + " - " + flags + " - " + path_parts.toString() + " metadata: " + metadata);

		final RecurseLFNs rl = new RecurseLFNs(null, operation, path, pat, index, path_parts, flags, metadata, null) {
			@Override
			public void notifyUp() {
				if (this.counter_left.decrementAndGet() == 0) {
					this.counter_left.decrementAndGet();
					synchronized (this) {
						this.notifyAll();
					}
				}
			}
		};
		try {
			tPool.submit(rl);
		}
		catch (final RejectedExecutionException ree) {
			logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit: " + ree);
			return false;
		}

		// lock and check counter
		synchronized (rl) {
			while (rl.counter_left.get() >= 0) {
				try {
					rl.wait(1 * 1000);
				}
				catch (final InterruptedException e) {
					logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't wait" + e);
				}
			}
		}

		return rl.critical_errors;
	}

	private static class RecurseLFNs implements Runnable {
		final RecursiveOp operation;
		final String base;
		final Pattern file_pattern;
		final int index;
		final ArrayList<String> parts;
		final int flags;
		final String metadata;
		final RecurseLFNs parent;
		boolean critical_errors = false;
		AtomicInteger counter_left = new AtomicInteger(0);
		LFN_CSD dir = null;
		LFN_CSD lfnc_dir = null;
		int submitted = 0;

		public RecurseLFNs(final RecurseLFNs parent, final RecursiveOp operation, final String base, final Pattern file_pattern, final int index, final ArrayList<String> parts, final int flags,
				final String metadata, final LFN_CSD lfnc_dir) {
			this.operation = operation;
			this.base = base;
			this.file_pattern = file_pattern;
			this.index = index;
			this.parts = parts;
			this.flags = flags;
			this.metadata = metadata;

			if (parent == null)
				this.parent = this;
			else
				this.parent = parent;

			if (lfnc_dir == null)
				dir = new LFN_CSD(base, true, append_table, null, null);
			else
				dir = lfnc_dir;
		}

		public void notifyUp() {
			if (this.counter_left.decrementAndGet() <= 0) {
				if ((!critical_errors && !operation.getOnlyAppend()) && !operation.callback(dir))
					critical_errors = true;
				if (critical_errors)
					parent.critical_errors = true;

				parent.notifyUp();
			}
		}

		@Override
		public String toString() {
			return base + index;
		}

		@Override
		public void run() {
			final boolean lastpart = (!operation.getRecurseInfinitely() && index >= parts.size());

			boolean includeDirs = false;
			if ((flags & LFNCSDUtils.FIND_INCLUDE_DIRS) != 0)
				includeDirs = true;

			if (!dir.exists || dir.type != 'd') {
				logger.severe("LFNCSDUtils recurseAndFilterLFNs: initial dir invalid - " + base);
				return;
			}

			final List<LFN_CSD> list = dir.list(true, append_table, clevel);

			// if the dir is empty, do the operation and notify
			if (list.isEmpty()) {
				if (!operation.getOnlyAppend() && !operation.callback(dir))
					parent.critical_errors = true;
				parent.notifyUp();
			}

			Pattern p;
			if (lastpart || operation.getRecurseInfinitely())
				p = this.file_pattern;
			else
				p = Pattern.compile(parts.get(index));

			JEP jep = null;

			if (metadata != null && !"".equals(metadata)) {
				jep = new JEP();
				jep.setAllowUndeclared(true);
				String expression = Format.replace(Format.replace(Format.replace(metadata, "and", "&&"), "or", "||"), ":", "__");
				expression = expression.replace("\"", "");
				jep.parseExpression(expression);
			}

			ArrayList<LFN_CSD> filesVersion = null;
			if ((flags & LFNCSDUtils.FIND_BIGGEST_VERSION) != 0)
				filesVersion = new ArrayList<>();

			// loop entries
			for (final LFN_CSD lfnc : list) {
				if (lfnc.type != 'd') {
					// no dir
					if (lastpart || operation.getRecurseInfinitely()) {
						// check pattern
						final Matcher m = p.matcher(operation.getRecurseInfinitely() ? lfnc.canonicalName : lfnc.child);
						if (m.matches()) {
							if (jep != null) {
								// we check the metadata of the file against our expression
								final Set<String> keys_values = new HashSet<>();

								// set the variable values from the metadata map
								for (final String s : lfnc.metadata.keySet()) {
									Double value;
									try {
										value = Double.valueOf(lfnc.metadata.get(s));
									}
									catch (final NumberFormatException e) {
										logger.info("Skipped: " + s + e);
										continue;
									}

									keys_values.add(s);
									jep.addVariable(s, value);
								}

								try {
									// This should return 1.0 or 0.0
									final Object result = jep.getValueAsObject();
									if (result instanceof Double && ((Double) result).intValue() == 1.0) {
										if (filesVersion != null)
											filesVersion.add(lfnc);
										else {
											if (!operation.callback(lfnc)) {
												parent.critical_errors = true;
											}
										}
									}
								}
								catch (final Exception e) {
									logger.info("RecurseLFNs metadata - cannot get result: " + e);
								}

								// unset the variables for the next lfnc to be processed
								for (final String s : keys_values) {
									jep.setVarValue(s, null);
								}
							}
							else {
								if (filesVersion != null)
									filesVersion.add(lfnc);
								else {
									if (!operation.callback(lfnc)) {
										parent.critical_errors = true;
									}
								}
							}
						}
					}
				}
				else {
					// dir
					if (lastpart || operation.getRecurseInfinitely()) {
						// if we already passed the hierarchy introduced on the command, all dirs are valid
						try {
							if (includeDirs) {
								final Matcher m = p.matcher(operation.getRecurseInfinitely() ? lfnc.canonicalName : lfnc.child);
								if (m.matches() && !operation.callback(lfnc)) {
									parent.critical_errors = true;
								}
							}
							if (operation.getRecurseInfinitely()) {
								// submit
								try {
									this.counter_left.incrementAndGet();
									submitted++;
									tPool.submit(new RecurseLFNs(this, operation, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, lfnc));
								}
								catch (final RejectedExecutionException ree) {
									logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit: " + ree);
								}
							}
						}
						catch (final RejectedExecutionException ree) {
							logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir(l) - " + base + lfnc.child + "/" + ": " + ree);
						}
					}
					else {
						// while exploring introduced dir, need to check patterns
						final Matcher m = p.matcher(lfnc.child);
						if (m.matches()) {
							// submit the dir
							try {
								this.counter_left.incrementAndGet();
								submitted++;
								tPool.submit(new RecurseLFNs(this, operation, base + lfnc.child + "/", file_pattern, index + 1, parts, flags, metadata, lfnc));
							}
							catch (final RejectedExecutionException ree) {
								logger.severe("LFNCSDUtils recurseAndFilterLFNs: can't submit dir - " + base + lfnc.child + "/" + ": " + ree);
							}
						}
					}
				}
			}

			// we filter and add the file if -y and metadata
			if (filesVersion != null) {
				final HashMap<String, Integer> lfn_version = new HashMap<>();
				final HashMap<String, LFN_CSD> lfn_to_csd = new HashMap<>();

				for (final LFN_CSD lfnc : filesVersion) {
					Integer version = Integer.valueOf(0);
					String lfn_without_version = lfnc.child;
					if (lfnc.child.lastIndexOf("_v") > 0) {
						lfn_without_version = lfnc.child.substring(0, lfnc.child.lastIndexOf("_v"));
						version = Integer.valueOf(Integer.parseInt(lfnc.child.substring(lfnc.child.indexOf("_v") + 2, lfnc.child.indexOf("_s0"))));
					}
					else if (lfnc.metadata.containsKey("CDB__version")) {
						version = Integer.valueOf(lfnc.metadata.get("CDB__version"));
					}

					if (!lfn_version.containsKey(lfn_without_version)) {
						lfn_version.put(lfn_without_version, version);
						lfn_to_csd.put(lfn_without_version, lfnc);
					}

					if (lfn_version.get(lfn_without_version).intValue() < version.intValue()) {
						lfn_version.put(lfn_without_version, version);
						lfn_to_csd.put(lfn_without_version, lfnc);
					}

				}

				for (final LFN_CSD lfn : lfn_to_csd.values())
					if (!operation.callback(lfn)) {
						parent.critical_errors = true;
					}

			}

			// check if there are submitted or notify
			if (submitted == 0 && !list.isEmpty()) {
				notifyUp();
			}

		}
	}

	/**
	 * @param base_path
	 * @param pattern
	 * @param flags
	 * @param metadata
	 * @return list of files for find command
	 */
	public static Collection<LFN_CSD> find(final String base_path, final String pattern, final int flags, final String metadata) {
		final Append ap = new Append();
		ap.setRecurseInfinitely(true);
		ap.setOnlyAppend(true);
		recurseAndFilterLFNs(ap, base_path, "*" + pattern, metadata, flags);
		return ap.getLfnsOk();
	}

	/**
	 * @param path
	 * @return list of files for ls command
	 */
	public static Collection<LFN_CSD> ls(final String path) {
		// if need to resolve wildcard and recurse, we call the recurse method
		if (path.contains("*") || path.contains("?")) {
			final Append ap = new Append();
			ap.setOnlyAppend(true);
			recurseAndFilterLFNs(ap, path, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);
			return ap.getLfnsOk();
		}

		// otherwise we should be able to create the LFN_CSD from the path
		final Set<LFN_CSD> ret = new TreeSet<>();
		final LFN_CSD lfnc = new LFN_CSD(path, true, append_table, null, null);
		if (lfnc.isDirectory()) {
			final List<LFN_CSD> list = lfnc.list(true, append_table, clevel);
			ret.addAll(list);
		}
		else {
			ret.add(lfnc);
		}

		return ret;
	}

	/**
	 * @param id
	 * @return LFN_CSD that corresponds to the id
	 */
	public static LFN_CSD guid2lfn(final UUID id) {

		String path = "";
		String lfn = "";
		UUID p_id = id;
		UUID p_id_lfn = null;
		HashMap<String, Object> p_c_ids = null;
		boolean first = true;

		while (!p_id.equals(LFN_CSD.root_uuid) && path != null) {
			p_c_ids = LFN_CSD.getInfofromChildId(p_id);

			if (p_c_ids != null) {
				path = (String) p_c_ids.get("path");
				p_id = (UUID) p_c_ids.get("path_id");

				if (first) {
					p_id_lfn = p_id;
					lfn = path;
					first = false;
				}
				else {
					lfn = path + "/" + lfn;
				}
			}
			else {
				logger.severe("LFN_CSD: guid2lfn: Can't get information for id: " + p_id + " - last lfn: " + lfn);
				return null;
			}
		}

		lfn = "/" + lfn;
		return new LFN_CSD(lfn, true, null, p_id_lfn, id);
	}

	/**
	 * @param user
	 * @param source
	 * @param destination
	 * @return final lfns moved and errors
	 */
	public static int mv(final AliEnPrincipal user, final String source, final String destination) {
		// Let's assume for now that the source and destination come as absolute paths, otherwise:
		// final String src = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), source);
		// final String dst = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), destination);
		if (source.equals(destination)) {
			logger.info("LFNCSDUtils: mv: the source and destination are the same: " + source + " -> " + destination);
			return 1;
		}

		final String[] destination_parts = LFN_CSD.getPathAndChildFromCanonicalName(destination);
		final LFN_CSD lfnc_target_parent = new LFN_CSD(destination_parts[0], true, null, null, null);
		final LFN_CSD lfnc_target = new LFN_CSD(destination, true, null, lfnc_target_parent.id, null);

		if (!lfnc_target_parent.exists) {
			logger.info("LFNCSDUtils: mv: the destination parent doesn't exist: " + destination);
			return 2;
		}
		if (!AuthorizationChecker.canWrite(lfnc_target_parent, user)) {
			logger.info("LFNCSDUtils: mv: no permission on the destination: " + destination);
			return 3;
		}

		// expand wildcards and filter if needed
		if (source.contains("*") || source.contains("?")) {
			final Move mv = new Move();
			mv.setUser(user);
			mv.setLfnTarget(lfnc_target);
			mv.setLfnTargetParent(lfnc_target_parent);
			recurseAndFilterLFNs(mv, source, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);
			return (mv.getLfnsError().isEmpty() ? 0 : 4);
		}

		final LFN_CSD lfnc_source = new LFN_CSD(source, true, null, null, null);
		// check permissions to move
		if (!AuthorizationChecker.canWrite(lfnc_source, user)) {
			logger.info("LFNCSDUtils: mv: no permission on the source: " + source);
			return 5;
		}
		// move and add to the final collection of lfns
		if (LFN_CSD.mv(lfnc_source, lfnc_target, lfnc_target_parent) == null) {
			logger.info("LFNCSDUtils: mv: failed to mv: " + source);
			return 6;
		}

		return 0;
	}

	/**
	 * @param user
	 * @param lfn
	 * @param purge
	 * @param recursive
	 * @param notifyCache
	 * @return final lfns deleted and errors
	 */

	public static boolean delete(final AliEnPrincipal user, final String lfn, final boolean purge, final boolean recursive, final boolean notifyCache) {
		// Let's assume for now that the lfn come as absolute paths, otherwise:
		// final String src = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), source);

		// expand wildcards and filter if needed
		if (lfn.contains("*") || lfn.contains("?") || recursive) {
			final Delete de = new Delete();
			de.setUser(user);
			recurseAndFilterLFNs(de, lfn, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);
			return de.getLfnsError().isEmpty();
		}

		final LFN_CSD lfnc = new LFN_CSD(lfn, true, null, null, null);
		// check permissions to rm
		if (!AuthorizationChecker.canWrite(lfnc, user)) {
			logger.info("LFNCSDUtils: rm: no permission to delete lfn: " + lfnc);
			return false;
		}
		// rm
		if (!lfnc.delete(purge, recursive, notifyCache)) {
			logger.info("LFNCSDUtils: rm: cannot delete lfn: " + lfnc);
			return false;
		}

		return true;
	}

	/**
	 * Touch an LFN_CSD: if the entry exists, update its timestamp, otherwise try to create an empty file
	 *
	 * @param user
	 *            who wants to do the operation
	 * @param lfnc
	 *            LFN_CSD to be touched
	 * @return <code>true</code> if the LFN was touched
	 */
	public static boolean touch(final AliEnPrincipal user, final LFN_CSD lfnc) {
		if (!lfnc.exists) {
			final LFN_CSD lfnc_parent = new LFN_CSD(lfnc.path, true, null, null, null);

			if (!AuthorizationChecker.canWrite(lfnc_parent, user)) {
				logger.log(Level.SEVERE, "Cannot write to the Current Directory OR Parent Directory is null BUT file exists. Terminating");
				return false;
			}

			lfnc.type = 'f';
			lfnc.size = 0;
			lfnc.checksum = "d41d8cd98f00b204e9800998ecf8427e";
			lfnc.parent_id = lfnc_parent.id;
			lfnc.owner = user.getName();
			lfnc.gowner = lfnc.owner;
			lfnc.perm = "755";
		}
		else if (!AuthorizationChecker.canWrite(lfnc, user)) {
			logger.log(Level.SEVERE, "LFNCSDUtils: touch: no permission to touch existing file: " + lfnc.getCanonicalName());
			return false;
		}

		final Date old_ctime = lfnc.ctime;
		lfnc.ctime = new Date();

		if (!lfnc.exists)
			return lfnc.insert();

		return lfnc.update(false, true, old_ctime);
	}

	/**
	 * Create a new directory (hierarchy) with a given owner
	 *
	 * @param owner
	 *            owner of the newly created structure(s)
	 * @param lfncs
	 *            the path to be created
	 * @param createMissingParents
	 *            if <code>true</code> then it will try to create any number of intermediate directories, otherwise the direct parent must already exist
	 * @return the (new or existing) directory, if the owner can create it, <code>null</code> if the owner is not allowed to do this operation
	 */
	public static LFN_CSD mkdir(final AliEnPrincipal owner, final String lfncs, final boolean createMissingParents) {
		if (owner == null || lfncs == null)
			return null;

		final LFN_CSD lfnc = new LFN_CSD(lfncs, true, null, null, null);

		if (lfnc.exists) {
			if (lfnc.isDirectory() && AuthorizationChecker.canWrite(lfnc, owner))
				return lfnc;

			return null;
		}

		lfnc.owner = owner.getName();
		lfnc.gowner = lfnc.owner;
		lfnc.size = 0;

		LFN_CSD parent = new LFN_CSD(lfnc.path, true, null, null, null);

		if (!parent.exists && !createMissingParents)
			return null;

		while (!parent.exists)
			parent = new LFN_CSD(parent.path, true, null, null, null);

		if (parent.isDirectory() && AuthorizationChecker.canWrite(parent, owner)) {
			boolean created = false;
			try {
				created = LFN_CSD.createDirectory(lfnc.canonicalName, null, ConsistencyLevel.QUORUM, lfnc.owner, lfnc.gowner, 0, "755", new Date());
			}
			catch (final Exception e) {
				logger.severe("LFNCSDUtils: mkdir: exception creating directory: " + lfnc.canonicalName + ": exception: " + e.toString());
			}

			if (created)
				return new LFN_CSD(lfnc.canonicalName, true, null, null, null);
		}

		return null;
	}

	/**
	 * @param path
	 * @param ses
	 * @param exses
	 * @param qos
	 * @param attempts
	 * @return transfer IDs to each SE
	 */
	public static HashMap<String, Long> mirror(final String path, final List<String> ses, final List<String> exses, final Map<String, Integer> qos, final Integer attempts) {
		final LFN_CSD lfnc = new LFN_CSD(path, true, null, null, null);

		if (!lfnc.exists || lfnc.pfns.size() <= 0)
			return null;

		// find closest SE
		final String site = ConfigUtils.getCloseSite();

		for (final Integer seNumber : lfnc.pfns.keySet()) {
			exses.add(SEUtils.getSE(seNumber).getName());
		}

		final List<SE> found_ses = SEUtils.getBestSEsOnSpecs(site, ses, exses, qos, true);
		final HashMap<String, Long> resmap = new HashMap<>();
		for (final SE s : found_ses) {
			final long transferID = attempts != null && attempts.intValue() > 0 ? TransferUtils.mirror(lfnc, s, null, attempts.intValue()) : TransferUtils.mirror(lfnc, s);
			resmap.put(s.getName(), Long.valueOf(transferID));
		}

		return resmap;
	}

	/**
	 * Change owner
	 *
	 * @param user
	 * @param lfn
	 * @param new_owner
	 * @param new_group
	 * @param recursive
	 * @return <code>true</code> if successful
	 */
	public static boolean chown(final AliEnPrincipal user, final String lfn, final String new_owner, final String new_group, final boolean recursive) {
		if (lfn == null || lfn.isEmpty() || new_owner == null || new_owner.isEmpty())
			return false;

		// Let's assume for now that the lfn come as absolute paths, otherwise:
		// final String src = FileSystemUtils.getAbsolutePath(user.getName(), (currentDir != null ? currentDir : null), source);

		// expand wildcards and filter if needed
		if (lfn.contains("*") || lfn.contains("?") || recursive) {
			final Chown ch = new Chown();
			ch.setUser(user);
			ch.setNewOwner(new_owner);
			ch.setNewGroup(new_group);
			ch.setRecurseInfinitely(true);
			recurseAndFilterLFNs(ch, lfn, null, null, LFNCSDUtils.FIND_INCLUDE_DIRS);
			return ch.getLfnsError().isEmpty();
		}

		final LFN_CSD lfnc = new LFN_CSD(lfn, true, null, null, null);
		// check permissions to chown
		if (!AuthorizationChecker.isOwner(lfnc, user)) {
			logger.info("LFNCSDUtils: chown: no permission to chown lfn: " + lfnc);
			return false;
		}
		// chown
		lfnc.owner = new_owner;
		lfnc.gowner = new_group;
		if (!lfnc.update(true, false, null)) {
			logger.info("LFNCSDUtils: chown: couldn't chown lfn: " + lfnc.getCanonicalName());
			return false;
		}

		return true;
	}

	/**
	 * @param path
	 * @param getFromDB
	 * @return LFN_CSD corresponding to path
	 */
	public static LFN_CSD getLFN(final String path, final boolean getFromDB) {
		return new LFN_CSD(path, getFromDB, null, null, null);
	}

	/**
	 * @param lfn
	 * @return true if the path fits a LFN path pattern
	 */
	public static boolean isValidLFN(final String lfn) {
		return Pattern.matches("^(/[^/]*)+./?$", lfn); // possibly that pattern should be refined
	}

	/**
	 * @param lfn_uuid
	 * @return true if the string fits a UUID pattern
	 */
	public static boolean isValidUUID(final String lfn_uuid) {
		try {
			UUID.fromString(lfn_uuid);
			return true;
		}
		catch (final Exception e) {
			logger.info("LFNCSDUtils: the string " + lfn_uuid + " is not a UUID: " + e.toString());
			return false;
		}
	}

}
