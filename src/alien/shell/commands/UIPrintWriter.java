package alien.shell.commands;


abstract class UIPrintWriter {

	/**
	 * Print stdout after appending line feed 
	 */
	abstract protected void printOutln(String line);

	/**
	 * Print stderr after appending line feed 
	 */
	abstract protected void printErrln(String line);
	
	/**
	 * Set the env for the client (needed for gapi)
	 */
	abstract protected void setenv(String env);
	
	/**
	 * Flush a set of lines as one transaction
	 */
	abstract protected void flush();
}
