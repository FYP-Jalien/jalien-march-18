package alien.io.xrootd;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import lia.util.process.ExternalProcessBuilder;
import lia.util.process.ExternalProcess.ExitStatus;

/**
 * @author costing
 *
 */
public class XrootdListing {

	/**
	 * Server host and port
	 */
	public final String server;
	
	/**
	 * Starting path
	 */
	public final String path;
	
	private Set<XrootdFile> entries;
	
	/**
	 * @param server server host and port
	 * @throws IOException 
	 */
	public XrootdListing(final String server) throws IOException{
		this(server, "/");
	}
	
	/**
	 * @param server server host and port
	 * @param path starting path
	 * @throws IOException 
	 */
	public XrootdListing(final String server, final String path) throws IOException{
		this.server = server;
		this.path = path;
		
		init();
	}
	
	private void init() throws IOException{
		entries = new TreeSet<XrootdFile>();
		
		final List<String> command = Arrays.asList("xrd", server, "ls "+path);
		
		final ExternalProcessBuilder pBuilder = new ExternalProcessBuilder(command);
		
        pBuilder.returnOutputOnExit(true);
        
        pBuilder.timeout(24, TimeUnit.HOURS);
        
        pBuilder.redirectErrorStream(true);
        
        final ExitStatus exitStatus;
        
        try{
        	exitStatus = pBuilder.start().waitFor();
        }
        catch (final InterruptedException ie){
        	throw new IOException("Interrupted while waiting for the following command to finish : "+command.toString());
        }
        
        if (exitStatus.getExtProcExitStatus() != 0){
        	// TODO something here or not ?
        }
        
        final BufferedReader br = new BufferedReader(new StringReader(exitStatus.getStdOut()));
        
        String sLine;
        
        while ( (sLine=br.readLine())!=null ){
        	if (sLine.startsWith("-") || sLine.startsWith("d")){
        		try{
        			entries.add(new XrootdFile(sLine.trim()));
        		}
        		catch (IllegalArgumentException iae){
        			System.err.println(iae.getMessage());
        		}
        	}
        }
	}
	
	/**
	 * @return the subdirectories of this entry
	 */
	public Set<XrootdFile> getDirs(){
		final Set<XrootdFile> ret = new TreeSet<XrootdFile>();
		
		for (final XrootdFile entry: entries){
			if (entry.isDirectory())
				ret.add(entry);
		}
		
		return ret;		
	}
	
	/**
	 * @return the files in this directory (or itself if it's a file already)
	 */
	public Set<XrootdFile> getFiles(){
		final Set<XrootdFile> ret = new TreeSet<XrootdFile>();
		
		for (final XrootdFile entry: entries){
			if (entry.isFile())
				ret.add(entry);
		}
		
		return ret;
	}
	
}
