package alien.site.supercomputing.titan;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import alien.api.catalogue.CatalogueApiUtils;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.io.IOUtils;
import alien.shell.commands.JAliEnCOMMander;

class FileDownloadApplication{
	List<LFN> fileList;
	List<Pair<LFN, String>> dlResult;
	
	FileDownloadApplication(List<LFN> inputFiles){
		fileList = inputFiles;
		dlResult = new LinkedList();
	}

	void putResult(LFN l, String s){
		dlResult.add(new Pair<LFN, String>(l,s));
	}

	boolean isCompleted(){
		return fileList.size()== dlResult.size();
	}
}

public class FileDownloadController extends Thread{
	private HashMap<LFN, LinkedList<FileDownloadApplication>> lfnRequested;
	//private Set<LFN> lfnQueueInProcess;

	private BlockingQueue<LFN> lfnToServe;

	private static String cacheFolder;
	private final int maxQueueCapacity = 1000;
	private final int maxDownloaderSleep = 10000;
	private final int maxParallelDownloads = 10;

	private static final Object poolSyncObject = new Object();
	private static int nowDownloadsRunning = 0;

	private final JAliEnCOMMander commander = JAliEnCOMMander.getInstance();
	private final CatalogueApiUtils c_api = new CatalogueApiUtils(commander);
	private ArrayList<Thread> dlPool;

	static private FileDownloadController instance = null;

	public static void setCacheFolder(String path){
		if(path!=null && path!=""){
			cacheFolder = path;
		}
	}

	/**
	 * The class which does actual download of LFNs
	 */
	private class FileDownloadThread extends Thread{
		public void run(){
			while(true){
				try{
					synchronized(poolSyncObject){
						poolSyncObject.wait(60000);
					}
					// if something is present in the 
					LFN l = lfnToServe.take();
					String dlFilename;
					if(l!=null){
						dlFilename = runDownload(l);
						// notify download finished
					}
				}
				catch(InterruptedException e){
					continue;
				}

			//	synchronized
			}
		}

		private String runDownload(LFN l){
			final List<PFN> pfns = c_api.getPFNsToRead( l, null, null);

			if (pfns == null || pfns.size() == 0) {
				//System.out.println("No replicas of " + entry.getKey().getCanonicalName() + 
				//						" to read from");
				System.out.println("No replicas of " + l.getCanonicalName() + 
										" to read from");
				return null;
			}

			final GUID g = pfns.iterator().next().getGuid();
			//commander.q_api.putJobLog(queueId, "trace", "Getting InputFile: " +
			//							entry.getKey().getCanonicalName());
			//final File f = IOUtils.get(g, entry.getValue());
			String dstFilename =  getCachedFilename(l);
			final File f = IOUtils.get(g, new File(dstFilename));

			if (f == null) {
				//System.out.println("Could not download " + entry.getKey().getCanonicalName() + 
				//				" to " + entry.getValue().getAbsolutePath());
				System.out.println("Could not download " + l.getCanonicalName() + 
								" to " + dstFilename); 
				return null;
			}
			return f.getName();
		}
	}

	public FileDownloadController getInstance(){
		try{
			if(instance==null){
				instance = new FileDownloadController();
			}
		}
		catch(Exception e){
			System.err.println("Exception caught on starting FileDownloadController: " + e.getMessage());
			return null;
		}
		return instance;
	}

	//private FileDownloadController(String cacheFolder) throws IOException, FileNotFoundException{
	private FileDownloadController() throws IOException, FileNotFoundException{
		if(cacheFolder==null || cacheFolder.equals(""))
			throw new IOException("Cache folder name can not be null");

		lfnRequested = new HashMap();
		lfnToServe = new LinkedBlockingQueue<>();

		// here to start a pool of threads
		dlPool = new ArrayList<>(maxParallelDownloads);
		for(int i=maxParallelDownloads; i>0; i--){
			FileDownloadThread fdt = new FileDownloadThread();
			dlPool.add(fdt);
			fdt.start();
		}
		
	}

	synchronized public FileDownloadApplication applyForDownload(List<LFN> inputFiles){
		FileDownloadApplication fda = new FileDownloadApplication(inputFiles);
		for(LFN l: inputFiles){
			if(lfnRequested.get(l)==null){
				LinkedList<FileDownloadApplication> dlAppList = new LinkedList<>();
				dlAppList.add(fda);
				lfnRequested.put(l, dlAppList);
				lfnToServe.add(l);
			}
			else{
				lfnRequested.get(l).add(fda);
			}
		}

		return fda;
	}

	public void run(){
		while(true){
			// if nothing in the lfnQueue -> sleep, continue
			boolean emptyQueue;
			synchronized(this){
				emptyQueue = lfnToServe.isEmpty();
			}
			if(emptyQueue){
				try{
					Thread.sleep(maxDownloaderSleep);
				}
				catch(InterruptedException e){}
				continue;
			}
			// tell the pool we got something
			synchronized(poolSyncObject ){
				poolSyncObject.notifyAll(); 
			}
		}
	}

	synchronized private void notifyCompleted(LFN l, String filename){
		for(FileDownloadApplication fda: lfnRequested.get(l)){
			fda.putResult(l, filename);
			if(filename==null){
				for(LFN lr: fda.fileList){
					lfnRequested.get(lr).remove(fda);
				}
			}
			if(fda.isCompleted() || filename==null){
				notifyCompletedFDA(fda);
			}
			lfnRequested.remove(l); 
		}
	}

	private void notifyCompletedFDA(FileDownloadApplication fda){
		synchronized(fda){
			fda.notify();
		}
	}

	private boolean fileIsInCache(LFN l){
		return new File(getCachedFilename(l)).exists();
	}

	public String getCachedFilename(LFN l){
		return cacheFolder + l.getCanonicalName();
	}


	private boolean checkMd5(){
		return true;
	}


	private boolean checkSize(LFN l) throws IOException {
		File f = new File(getCachedFilename(l));
		return l.size==f.length();
	}
}
