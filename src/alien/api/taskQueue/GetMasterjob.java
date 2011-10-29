package alien.api.taskQueue;

import java.util.List;

import alien.api.Request;
import alien.taskQueue.Job;
import alien.user.AliEnPrincipal;

/**
 * Get a JDL object
 * 
 * @author ron
 * @since Jun 05, 2011
 */
public class GetMasterjob extends Request {



	/**
	 * 
	 */
	private static final long serialVersionUID = -453943843524253526L;

	/**
	 * 
	 */
	private List<Job> jobs = null;

	private final String jobId;
	
	private final String status;
	
	private final String id;
	
	private final String site;
	
	private final boolean bPrintId;

	private final boolean bPrintSite;

	private final boolean bMerge;

	private final boolean bKill;

	private final boolean bResubmit;

	private final boolean bExpunge;
	
	/**
	 * @param user 
	 * @param role 
	 * @param jobId 
	 * @param status 
	 * @param id 
	 * @param site 
	 * @param bPrintId 
	 * @param bPrintSite 
	 * @param bMerge 
	 * @param bKill 
	 * @param bResubmit 
	 * @param bExpunge 
	 */
	public GetMasterjob(final AliEnPrincipal user, final String role, final String jobId, final String status, final String id, final String site,
			final boolean bPrintId, final boolean bPrintSite, final boolean bMerge, final boolean bKill, final boolean bResubmit, final boolean bExpunge){
		setRequestUser(user);
		setRoleRequest(role);
		this.jobId = jobId;
		this.status = status;
		this.id = id;
		this.site = site;
		this.bPrintId = bPrintId;
		this.bPrintSite = bPrintSite;
		this.bMerge = bMerge;
		this.bKill = bKill;
		this.bResubmit = bResubmit;
		this.bExpunge = bExpunge;
	}
	
	
	@Override
	public void run() {
		//this.jobs = 
	}
	
	/**
	 * @return a JDL
	 */
	public List<Job> returnMasterjob(){
		if(bKill)
			System.out.println("status: " + jobId );
		return this.jobs;
	}
	
	@Override
	public String toString() {
		return "Asked for Masterjob :  reply is: "+this.jobs;
	}
}
