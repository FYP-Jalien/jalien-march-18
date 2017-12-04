package alien.test.utils;

import alien.api.DispatchSSLServer;
import alien.api.JBoxServer;
import alien.test.chapters.TestJShOverJBox;
import alien.user.JAKeyStore;

/**
 * @author ron
 * @since Oct 25, 2011
 */
public class TestService extends Thread{

	private boolean jBox = false;
		
	private boolean jCentral = false;
	
	private boolean jSh = false;
	
	private int status = 0;
	
	/**
	 * @param service
	 */
	public TestService(String service){
		if("jcentral".equals(service))
			jCentral = true;
		else if("jbox".equals(service))
			jBox = true;
		else if("jsh".equals(service))
			jSh = true;
	}
	
	@Override
	public void run(){
		if(jCentral)
			startJCentral();
		else if(jBox)
			startJBox();
		else if(jSh)
			startJBox();		
	}
	
	/**
	 * @return status
	 */
	public int getStatus(){
		return status;
	}
	

	/**
	 * jCentral started successfully
	 */
	public void startJCentral(){
	//	logger.setLevel(Level.WARNING);

		try {
//			SimpleCatalogueApiService catalogueAPIService = new SimpleCatalogueApiService();
//			catalogueAPIService.start();
			JAKeyStore.loadKeyStore();
			DispatchSSLServer.runService();
		} catch (Exception e) {
			e.printStackTrace();
			status = -1;
		}
		
	}
	
	/**
	 * jBox started successfully
	 */
	public void startJBox(){
		
		try {
			JAKeyStore.loadKeyStore();
			JBoxServer.startJBoxServer(0);
		}
		
		catch (Exception e) {
			e.printStackTrace();
			status = -1;
		}
	}
	
	/**
	 * jSh started successfully
	 */
	public void startJSh(){
		
		try {
			TestJShOverJBox.runTestChapter();
		}
		
		catch (Exception e) {
			e.printStackTrace();
			status = -1;
		}
		
	}
	
}
