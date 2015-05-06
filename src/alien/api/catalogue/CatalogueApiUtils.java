package alien.api.catalogue;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import alien.api.Dispatcher;
import alien.api.ServerException;
import alien.catalogue.GUID;
import alien.catalogue.LFN;
import alien.catalogue.PFN;
import alien.catalogue.Package;
import alien.catalogue.access.AccessType;
import alien.config.ConfigUtils;
import alien.se.SE;
import alien.shell.commands.JAliEnCOMMander;

/**
 * 
 * @author ron
 * @since Jun 03, 2011
 */
public class CatalogueApiUtils {

	/**
	 * Logger
	 */
	static transient final Logger logger = ConfigUtils.getLogger(CatalogueApiUtils.class.getCanonicalName());

	private final JAliEnCOMMander commander;

	/**
	 * @param commander
	 */
	public CatalogueApiUtils(final JAliEnCOMMander commander) {
		this.commander = commander;
	}

	/**
	 * Get LFN from String, only if it exists
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN object
	 */
	public LFN getLFN(final String slfn) {
		return getLFN(slfn, false);
	}

	/**
	 * Get LFNs from String as a directory listing, only if it exists
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @return the LFN objects
	 */
	public List<LFN> getLFNs(final String slfn) {
		try {
			return Dispatcher.execute(new LFNListingfromString(commander.getUser(), commander.getRole(), slfn)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get LFN from String
	 * 
	 * @param slfn
	 *            name of the LFN
	 * @param evenIfDoesNotExist
	 * @return the LFN object
	 */
	public LFN getLFN(final String slfn, final boolean evenIfDoesNotExist) {

		try {
			return Dispatcher.execute(new LFNfromString(commander.getUser(), commander.getRole(), slfn, evenIfDoesNotExist)).getLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get LFN: " + slfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a LFN in the Catalogue
	 * 
	 * @param path
	 * @return state of the LFN's deletion <code>null</code>
	 */
	public boolean removeLFN(final String path) {
		try {
			return Dispatcher.execute(new RemoveLFNfromString(commander.getUser(), commander.getRole(), path)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the LFN: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * Move a LFN in the Catalogue
	 * 
	 * @param path
	 * @param newpath
	 * @return state of the LFN's deletion <code>null</code>
	 */
	public LFN moveLFN(final String path, final String newpath) {
		try {
			return Dispatcher.execute(new MoveLFNfromString(commander.getUser(), commander.getRole(), path, newpath)).newLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not move the LFN-->newLFN: " + path + "-->" + newpath);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get GUID from String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @return the GUID object
	 */
	public GUID getGUID(final String sguid) {
		return getGUID(sguid, false);
	}

	/**
	 * Get GUID from String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @param evenIfDoesNotExist
	 * @return the GUID object
	 */
	public GUID getGUID(final String sguid, final boolean evenIfDoesNotExist) {
		try {
			return Dispatcher.execute(new GUIDfromString(commander.getUser(), commander.getRole(), sguid, evenIfDoesNotExist)).getGUID();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get PFNs from GUID as String
	 * 
	 * @param sguid
	 *            GUID as String
	 * @return the PFNs
	 */
	public Set<PFN> getPFNs(final String sguid) {
		try {
			return Dispatcher.execute(new PFNfromString(commander.getUser(), commander.getRole(), sguid)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get GUID: " + sguid);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get PFNs for reading by LFN
	 * 
	 * @param lfn
	 *            LFN of the entry as String
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to deprioritize to read from
	 * @return PFNs, filled with read envelopes and credentials if necessary and authorized
	 */
	public List<PFN> getPFNsToRead(final LFN lfn, final List<String> ses, final List<String> exses) {
		try {

			return Dispatcher.execute(new PFNforReadOrDel(commander.getUser(), commander.getRole(), commander.getSite(), AccessType.READ, lfn, ses, exses)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();

		}
		return null;
	}

	/**
	 * Get PFNs for writing by LFN
	 * 
	 * @param lfn
	 *            LFN of the entry as String
	 * @param guid
	 * @param ses
	 *            SEs to prioritize to read from
	 * @param exses
	 *            SEs to de-prioritize
	 * @param qos
	 *            QoS types and counts to ask for
	 * @return PFNs, filled with write envelopes and credentials if necessary and authorized
	 */
	public List<PFN> getPFNsToWrite(final LFN lfn, final GUID guid, final List<String> ses, final List<String> exses, final HashMap<String, Integer> qos) {
		try {
			return Dispatcher.execute(new PFNforWrite(commander.getUser(), commander.getRole(), commander.getSite(), lfn, guid, ses, exses, qos)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFN for: " + lfn);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Register PFNs with envelopes
	 * 
	 * @param envelopes
	 * @return PFNs that were successfully registered
	 */
	public List<PFN> registerEnvelopes(final List<String> envelopes) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), commander.getRole(), envelopes)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + envelopes.toString());
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Register PFNs with enveloeps
	 * 
	 * @param encryptedEnvelope
	 * @param size
	 * @param md5
	 * @param lfn
	 * @param perm
	 * @param expire
	 * @param pfn
	 * @param se
	 * @param guid
	 * @return PFNs that were successfully registered
	 */
	public List<PFN> registerEncryptedEnvelope(final String encryptedEnvelope, final int size, final String md5, final String lfn, final String perm, final String expire, final String pfn,
			final String se, final String guid) {
		try {
			return Dispatcher.execute(new RegisterEnvelopes(commander.getUser(), commander.getRole(), encryptedEnvelope, size, md5, lfn, perm, expire, pfn, se, guid)).getPFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get PFNs for: " + encryptedEnvelope);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 * 
	 * @param path
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path) {
		return createCatalogueDirectory(path, false);
	}
	
	/**
	 * 
	 * @param path
	 * @return LFN of the created file, if successful, else <code>null</code>
	 */
	public LFN touchLFN(final String path){
		try {
			return Dispatcher.execute(new TouchLFNfromString(commander.getUser(), 
									commander.getRole(), path )).getLFN();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the file: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Create a directory in the Catalogue
	 * 
	 * @param path
	 * @param createNonExistentParents
	 * @return LFN of the created directory, if successful, else <code>null</code>
	 */
	public LFN createCatalogueDirectory(final String path, final boolean createNonExistentParents) {
		try {
			return Dispatcher.execute(new CreateCatDirfromString(commander.getUser(), commander.getRole(), path, createNonExistentParents)).getDir();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not create the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Remove a directory in the Catalogue
	 * 
	 * @param path
	 * @return state of directory's deletion <code>null</code>
	 */
	public boolean removeCatalogueDirectory(final String path) {
		try {
			return Dispatcher.execute(new RemoveCatDirfromString(commander.getUser(), commander.getRole(), path)).wasRemoved();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not remove the CatDir: " + path);
			e.getCause().printStackTrace();
		}

		return false;
	}

	/**
	 * find bases on pattern
	 * 
	 * @param path
	 * @param pattern
	 * @param flags
	 * @return result LFNs
	 */
	public Collection<LFN> find(final String path, final String pattern, final int flags) {
		try {
			return Dispatcher.execute(new FindfromString(commander.getUser(), commander.getRole(), path, pattern, flags)).getLFNs();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Unable to execute find: path (" + path + "), pattern (" + pattern + "), flags (" + flags + ")");
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get an SE by its name
	 * 
	 * @param se
	 *            name of the SE
	 * @return SE object
	 */
	public SE getSE(final String se) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), commander.getRole(), se)).getSE();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + se);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get an SE by its number
	 * 
	 * @param seno
	 *            number of the SE
	 * @return SE object
	 */
	public SE getSE(final int seno) {
		try {
			return Dispatcher.execute(new SEfromString(commander.getUser(), commander.getRole(), seno)).getSE();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get SE: " + seno);
			e.getCause().printStackTrace();
		}

		return null;
	}

	/**
	 * Get Packages for a certain platform
	 * 
	 * @param platform
	 * @return the Packages
	 */
	public List<Package> getPackages(final String platform) {
		try {
			return Dispatcher.execute(new PackagesfromString(commander.getUser(), commander.getRole(), platform)).getPackages();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not get Packages for: " + platform);
			e.getCause().printStackTrace();
		}

		return null;
	}
	
	public LFN chownLFN( String lfn_name, String username_to_chown, String groupname_to_chown ){
		if( lfn_name == "" || lfn_name == null )
			return null;
		LFN lfn = this.getLFN( lfn_name, false );
		if( lfn == null )
			return null;
		try {
			ChownLFN cl = Dispatcher.execute( new ChownLFN( commander.getUser(), 
															commander.getRole(), lfn_name, 
															username_to_chown, 
															groupname_to_chown ) );
			if( cl.getSuccess() )
				return lfn;
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Could not chown " + lfn_name + " for " + username_to_chown);
			e.getCause().printStackTrace();
		}
		return null;
	}
	
	
	public int mirrorLFN( String lfn_name,
								String dstSE,
								//boolean keepSamePath,
								boolean useLFNasGuid,
								//boolean checkFileIsPresentOnDest,
								//boolean transferWholeArchive,							
								//Integer masterTransferId,
								Integer attempts){
		
		if( lfn_name == "" || lfn_name == null )
			return -320;
		LFN lfn = this.getLFN( lfn_name, false );
		if( lfn == null )
			return -330;
		
		try {
			MirrorLFN ml = Dispatcher.execute( new MirrorLFN( commander.getUser(), 
															commander.getRole(), 
															lfn_name, 
															dstSE,
															//keepSamePath,
															useLFNasGuid,
															//checkFileIsPresentOnDest,
															//transferWholeArchive,
															//masterTransferId,
															attempts ) );
			return ml.getResult();
		}
		catch (final ServerException e) {
			logger.log(Level.WARNING, "Problems mirroring LFN");
			e.getCause().printStackTrace();
		}
		
		return -350;
	}
}
