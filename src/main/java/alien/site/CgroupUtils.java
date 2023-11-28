package alien.site;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import lazyj.commands.SystemCommand;

/**
 * @author mstoretv
 */
public class CgroupUtils {

	/**
	 * 
	 * @return true if system uses cgroupsv2
	 */
	public static boolean haveCgroupsv2() {
		return (new File("/sys/fs/cgroup/cgroup.controllers").isFile());
	}

	/**
	 * 
	 * Creates new cgroups from current root for JobRunner ("runner") and JobAgents/JobWrappers ("agents"). Also
	 * moves JobRunner to "runner", and delegates controllers.
	 * 
	 * @param runnerPid pid for JobRunner
	 * @return true if both cgroups were created, and process successfully moved
	 */
	public static boolean setupTopCgroups(int runnerPid) {
		if (haveCgroupsv2()) {
			final String slotCgroup = getCurrentCgroup(runnerPid);

			if (createCgroup(slotCgroup, "runner") && createCgroup(slotCgroup, "agents")) {
				try {
					final String procsToMove = Files.readString(Paths.get(slotCgroup + "/cgroup.procs"));
					Arrays.stream(procsToMove.split("\\r?\\n")).forEach(line -> moveProcessToCgroup(slotCgroup + "/runner", Integer.parseInt(line)));

					// TODO: delegate all controllers
					SystemCommand.bash("echo +memory >> " + slotCgroup + "/cgroup.subtree_control");
					SystemCommand.bash("echo +memory >> " + slotCgroup + "/agents/cgroup.subtree_control");

					return true;
				}
				catch (final Exception e) {
					// Ignore
				}
			}
		}
		return false;
	}

	/**
	 * 
	 * Creates a new cgroup given a valid path and name
	 * 
	 * @param name name of the new cgroup to be created
	 * @param dir location in the fs tree for the new cgroup
	 * @return true if cgroup was created
	 */
	public static boolean createCgroup(String dir, String name) {
		if (!dir.contains("/sys/fs/cgroup"))
			return false;

		final File newCgroup = new File(dir + "/" + name);
		newCgroup.mkdir();

		return newCgroup.exists();
	}

	/**
	 * 
	 * Moves a process to a new cgroup
	 * 
	 * @param cgroup destination cgroup
	 * @param pid process pid
	 * @return true if process was moved
	 */
	public static boolean moveProcessToCgroup(String cgroup, int pid) {
		try {
			Files.writeString(Paths.get(cgroup + "/cgroup.procs"), String.valueOf(pid), StandardOpenOption.APPEND);
			return Files.readString(Paths.get(cgroup + "/cgroup.procs")).contains(String.valueOf(pid));
		}
		catch (final Exception e) {
			return false;
		}
	}

	/**
	 * 
	 * Returns the cgroup of a given pid
	 * 
	 * @param pid
	 * @return String path of cgroup in the fs
	 */
	public static String getCurrentCgroup(int pid) {
		try {
			String groups[] = Files.readString(Paths.get("/proc/" + pid + "/cgroup")).split(System.lineSeparator());
			for (String group : groups) {
				if (!group.contains("freezer"))
					return "/sys/fs/cgroup" + group.split(":")[2];
			}
		}
		catch (final Exception e) {
			// Ignore
		}
		return "";
	}

	/**
	 * 
	 * Sets memory.high for cgroup of given pid, if possible
	 * 
	 * @param pid
	 * @param limit as string with size + unit (e.g. 500M)
	 */
	public static void setMemoryHigh(int pid, String limit) {
		try {
			Files.writeString(Paths.get(getCurrentCgroup(pid) + "/memory.high"), limit, StandardOpenOption.WRITE);
		}
		catch (IOException e) {
			// Ignore
		}
	}

	/**
	 * 
	 * Sets memory.max for cgroup of given pid, if possible
	 * 
	 * @param pid
	 * @param limit as string with size + unit (e.g. 500M)
	 */
	public static void setMemoryMax(int pid, String limit) {
		try {
			Files.writeString(Paths.get(getCurrentCgroup(pid) + "/memory.max"), limit, StandardOpenOption.WRITE);
		}
		catch (IOException e) {
			// Ignore
		}
	}

	/**
	 * 
	 * Deletes all cgroups from the root of the given pid
	 * 
	 * @param pid
	 * @return true if no errors when
	 */
	public static boolean cleanupCgroups(int pid) {
		try {
			// TODO: delete recursively if cgroup.kill is not available.
			Files.writeString(Paths.get(getCurrentCgroup(pid) + "/cgroup.kill"), "1", StandardOpenOption.WRITE);
		}
		catch (IOException e) {
			return false;
		}
		return true;
	}
}