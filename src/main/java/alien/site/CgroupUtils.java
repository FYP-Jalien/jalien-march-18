package alien.site;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	 * @param runnerPid
	 * @return true if both cgroups were created, and process successfully moved
	 */
	public static boolean setupTopCgroups(int runnerPid) {
		if (haveCgroupsv2()) {
			final String slotCgroup = getCurrentCgroup(runnerPid);

			if (createCgroup(slotCgroup, "runner") && createCgroup(slotCgroup, "agents")) {
				try {
					final String procsToMove = Files.readString(Paths.get(slotCgroup + "/cgroup.procs"));
					Arrays.stream(procsToMove.split("\\r?\\n")).forEach(line -> SystemCommand.bash("echo " + line + " >> " + slotCgroup + "/runner/cgroup.procs"));

					//TODO: delegate all controllers
					SystemCommand.bash("echo +memory >> " + slotCgroup + "/cgroup.subtree_control");
					SystemCommand.bash("echo +memory >> " + slotCgroup + "/agents/cgroup.subtree_control");

					return true;
				}
				catch (final Exception e) {
					return false;
				}
			}
		}
		return false;
	}

/**
 * 
 * Creates a new cgroup given a directory
 * 
 * @param name name of the new cgroup to be created
 * @param dir location in the fs tree for the new cgroup
 * @return true if cgroup was created
 */
	public static boolean createCgroup(String dir, String name){
		final File newCgroup = new File(dir + "/" + name);
		newCgroup.mkdir();

		if (!newCgroup.exists())
			return false;
		else
			return true;
	}

	/**
	 * 
	 * Moves a process to a new cgroup
	 * 
	 * @param cgroup destination cgroup
	 * @param pid process pid
	 * @return true if process was moved
	 */
	public static boolean moveProcessToCgroup(String cgroup, int pid){
		try {
			SystemCommand.bash("echo " + pid + " >> " + cgroup + "/cgroup.procs");	
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
	 * @return
	 */
	public static String getCurrentCgroup(int pid) {
		try {
			return SystemCommand.bash("echo /sys/fs/cgroup$(cat /proc/" + pid + "/cgroup | grep slot | cut -d \":\" -f 3)").stdout;
		}
		catch (final Exception e) {
			return "";
		}
	}

}
