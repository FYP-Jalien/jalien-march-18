package utils.lfncrawler;

import alien.catalogue.CatalogueUtils;
import alien.catalogue.IndexTableEntry;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import alien.optimizers.Optimizer;
import alien.optimizers.DBSyncUtils;
import lazyj.DBFunctions;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * LFN Crawler
 *
 * @author ibrinzoi
 *
 */
public class LFNCrawler extends Optimizer {
    /**
     * Instance
     */
    private static final LFNCrawler lfnCrawlerInstance = new LFNCrawler();

    /**
	 * Logger
	 */
    private static final Logger logger = ConfigUtils.getLogger(LFNCrawler.class.getCanonicalName());

    /**
	 * Crawler Frequency
	 */
    private static final int ONE_HOUR  = 3600 * 1000;
    private static int       frequency = 24 * ONE_HOUR; // one day

    /**
	 * Dry Run Mode
	 */
    private static boolean dryRun;

    /**
	 * Current Date
	 */
    private static Date currentDate;

    /**
	 * Statistics
	 */    
    private static long    directoriesDeleted;
    private static long    filesDeleted;
    private static long    reclaimedSpace;
    private static Instant startTime;
    private static Instant endTime;
    private static long    elapsedTime;


    /**
	 * Private Constructor as the class is implemented as a Singleton
	 */
    private LFNCrawler() {
    }

    /**
	 * Return the instance of the class
	 */
    public static LFNCrawler getLFNCrawlerInstance() {
        return lfnCrawlerInstance;
    }

    @Override
    public void run() {
        directoriesDeleted = 0;
        filesDeleted       = 0;
        reclaimedSpace     = 0;
        currentDate        = new Date();
        dryRun             = ConfigUtils.getConfig().getb("lfn_crawler_dry_run", true);

        this.setSleepPeriod(ONE_HOUR);
        DBSyncUtils.checkLdapSyncTable();

        while (true) {
            final boolean updated = DBSyncUtils.updatePeriodic(frequency, LFNCrawler.class.getCanonicalName());

            if (updated) {
                print("Starting expired LFN crawling iteration in " + (dryRun ? "DRY-RUN" : "NORMAL") + " mode");

                startCrawler();
            }

            try {
                print("LFNCrawler sleeps " + this.getSleepPeriod());
                sleep(this.getSleepPeriod());
            }
            catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
	 * Iterate through the expired directories and delete
     * them recursively starting from the top-most parent
	 *
	 * @param indextableCollection A collection of indexTables on which to perform the queries
	 * @return void
	 */
    private static void removeDirectories(Collection<IndexTableEntry> indextableCollection) {
        LFN currentDirectory = null;

        for (final IndexTableEntry ite : indextableCollection) {

            try (DBFunctions db = ite.getDB()) {
                db.query("set wait_timeout=31536000;");

                String q = "SELECT * FROM L" + ite.tableName + "L WHERE expiretime IS NOT NULL AND expiretime < ? AND type = 'd' ORDER BY lfn";

                db.setReadOnly(true);
                if (!db.query(q, false, currentDate)) {
                    print("db.query returned error");
                    return;
                }

                while (db.moveNext()) {
                    final LFN lfn = new LFN(db, ite);

                    if (currentDirectory == null) {
                        currentDirectory = lfn;
                    } else if (!lfn.getCanonicalName().contains(currentDirectory.getCanonicalName() + "/")) {
                        print("Removing directory recursively: " + currentDirectory.getCanonicalName());
                        if (!dryRun) {
                            currentDirectory.delete(true, true);
                        }

                        reclaimedSpace  += currentDirectory.size;
                        directoriesDeleted++;
                        currentDirectory = lfn;
                    } else {
                        print("Found subentry: " + lfn.getCanonicalName());
                    }
                }

                // Remove the last directory
                if (currentDirectory != null) {
                    print("Removing directory recursively: " + currentDirectory.getCanonicalName());
                    if (!dryRun) {
                        currentDirectory.delete(true, true);
                    }

                    reclaimedSpace  += currentDirectory.size;
                    directoriesDeleted++;
                    currentDirectory = null;
                }

            } catch (@SuppressWarnings("unused") final Exception e) {
                // ignore
            }

            DBSyncUtils.setLastActive(LFNCrawler.class.getCanonicalName());
        }
    }

    /**
	 * Iterate through a list of LFNs, check if any is an archive so that
     * the members would be added to the list too and delete the final batch
	 *
	 * @param lfnsToDelete A list of LFNs to be parsed and deleted
	 * @return void
	 */
    public static void processBatch(ArrayList<LFN> lfnsToDelete) {
        Set<LFN> processedToDelete = new HashSet<>();

        for (final LFN l : lfnsToDelete) {
            if (processedToDelete.contains(l)) {
                continue;
            }

            final LFN realLFN = LFNUtils.getRealLFN(l);
            if (processedToDelete.contains(realLFN)) {
                continue;
            }

            print("Parsing LFN: " + l.getCanonicalName());

            final List<LFN> members = LFNUtils.getArchiveMembers(realLFN);
            if (members != null) {
                for (final LFN member : members) {
                    processedToDelete.add(member);
                    print("Found archive member: " + member.getCanonicalName());
                }
            }

            processedToDelete.add(l);
            if (realLFN != null) {
                processedToDelete.add(realLFN);
            }
        }

        for (final LFN l : processedToDelete) {
            print("Removing LFN: " + l.getCanonicalName());
            if (!dryRun) {
                l.delete(true, false);
            }

            reclaimedSpace += l.size;
        }

        filesDeleted += processedToDelete.size();
    }

    /**
	 * Iterate through the expired files and process
     * them in batches, parsing one directory at a time
	 *
	 * @param indextableCollection A collection of indexTables on which to perform the queries
	 * @return void
	 */
    private static void removeFiles(Collection<IndexTableEntry> indextableCollection) {
        String currentDirectory     = null;
        ArrayList<LFN> lfnsToDelete = new ArrayList<>();

        for (final IndexTableEntry ite : indextableCollection) {

            try (DBFunctions db = ite.getDB()) {
                db.query("set wait_timeout=31536000;");

                String q = "SELECT * FROM L" + ite.tableName + "L WHERE expiretime IS NOT NULL AND expiretime < ? AND type = 'f' ORDER BY lfn";

                db.setReadOnly(true);
                if (!db.query(q, false, currentDate)) {
                    print("db.query returned error");
                    return;
                }

                while (db.moveNext()) {
                    final LFN lfn = new LFN(db, ite);
                    final String parent = lfn.getParentName();

                    if (currentDirectory == null) {
                        currentDirectory = parent;

                        print("Entering directory: " + currentDirectory);
                    } else if (!currentDirectory.equals(parent)) {
                        processBatch(lfnsToDelete);
                        lfnsToDelete = new ArrayList<>();

                        currentDirectory = parent;

                        print("Entering directory: " + currentDirectory);
                    }
                    lfnsToDelete.add(lfn);
                }

                // Process the last batch
                if (!lfnsToDelete.isEmpty()) {
                    processBatch(lfnsToDelete);
                    lfnsToDelete = new ArrayList<>();
                }

            } catch (@SuppressWarnings("unused") final Exception e) {
                // ignore
            }

            DBSyncUtils.setLastActive(LFNCrawler.class.getCanonicalName());
        }
    }

    /**
	 * Print a message to Standard Output
	 *
	 * @param message The message to print
	 * @return void
	 */
    private static void print(String message) {
        logger.log(Level.INFO, message);
    }

    /**
	 * Start the LFN Crawler
	 */
    public static void startCrawler() {
        startTime = Instant.now();

        final Collection<IndexTableEntry> indextableCollection = CatalogueUtils.getAllIndexTables();

        if (indextableCollection == null) {
            print("indextableCollection is null");
            return;
        }

        print("========== Directories iteration ==========");
        removeDirectories(indextableCollection);

        print("========== Files iteration ==========");
        removeFiles(indextableCollection);

        endTime     = Instant.now();
        elapsedTime = Duration.between(startTime, endTime).toHours();

        print("========== Results ==========");
        print("Directories deleted: " + directoriesDeleted);
        print("Files deleted: "       + filesDeleted);
        print("Reclaimed space: "     + reclaimedSpace);
        print("Execution took: "      + elapsedTime + " hours");

        DBSyncUtils.registerLog(LFNCrawler.class.getCanonicalName(),
                                "Directories deleted: " + directoriesDeleted + "\n" +
                                "Files deleted: "       + filesDeleted       + "\n" +
                                "Reclaimed space: "     + reclaimedSpace     + "\n" +
                                "Execution took: "      + elapsedTime        + " hours");
    }
}