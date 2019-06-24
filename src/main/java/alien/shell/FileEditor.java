package alien.shell;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcess.ExitStatus;
import lia.util.process.ExternalProcessBuilder;

/**
 * @author ron
 * @since November 11, 2011
 */
public class FileEditor {
	/**
	 * Available editors
	 */
	public static final String[] editors = { "sensible-editor", "edit", "mcedit", "vim", "joe", "emacs", "more", "less", "nano" };

	private static final Map<String, String> editorCommands = new TreeMap<>();

	static {
		for (final String editor : editors) {
			final String path = which(editor);

			if (path != null)
				editorCommands.put(editor, path);
		}
	}

	private final String editor;

	/**
	 * @return list of available editor commands
	 */
	public static final List<String> getAvailableEditorCommands() {
		final List<String> ret = new ArrayList<>();

		ret.addAll(editorCommands.keySet());

		return ret;
	}

	/**
	 * @param editorname
	 * @throws IOException
	 */
	public FileEditor(final String editorname) throws IOException {
		editor = editorCommands.get(editorname);

		if (editor == null)
			throw new IOException(editorname + ": command not found");
	}

	/**
	 * @param editorname
	 * @return <code>true</code> if this editor is available
	 */
	public static boolean isEditorCommand(final String editorname) {
		return editorCommands.containsKey(editorname);
	}
	
	/**
	 * @param filename
	 * @return exit code from the executed application
	 */
	public int edit(final String filename) {
		final ProcessBuilder pb = new ProcessBuilder(editor, filename);

		pb.redirectInput(ProcessBuilder.Redirect.INHERIT);
		pb.redirectError(ProcessBuilder.Redirect.INHERIT);
		pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);

		try {
			final Process p = pb.start();

			p.waitFor();

			final int code = p.exitValue();

			return code;
		} catch (@SuppressWarnings("unused") final Throwable t) {
			return -1;
		}
	}

	private static String which(final String command) {
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(Arrays.asList("which", command));

		pBuilder.returnOutputOnExit(true);

		pBuilder.timeout(7, TimeUnit.SECONDS);
		try {
			final ExitStatus exitStatus = pBuilder.start().waitFor();

			if (exitStatus.getExtProcExitStatus() == 0)
				return exitStatus.getStdOut().trim();

		} catch (@SuppressWarnings("unused") final Exception e) {
			// ignore
		}

		// System.err.println("Command [" + command + "] not found.");
		return null;
	}
}
