package utils.lfncrawler;

import alien.catalogue.CatalogueUtils;
import alien.catalogue.IndexTableEntry;
import alien.catalogue.LFN;
import alien.catalogue.LFNUtils;
import alien.config.ConfigUtils;
import lazyj.DBFunctions;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LFNCrawler {
    private static Date             currentDate;
    private static final LFNCrawler lfnCrawlerInstance = new LFNCrawler();
    private static final Logger     logger             = ConfigUtils.getLogger(LFNCrawler.class.getCanonicalName());

    private LFNCrawler() {

    }

    public static LFNCrawler getLFNCrawlerInstance() {
        return lfnCrawlerInstance;
    }

    public static void main(String[] args) {
        currentDate = new Date();

        print("Starting expired LFN crawling iteration");

        startCrawler();
    }

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

                        currentDirectory = lfn;
                    } else {
                        print("Found subentry: " + lfn.getCanonicalName());
                    }
                }

                // Remove the last directory
                if (currentDirectory != null) {
                    print("Removing directory recursively: " + currentDirectory.getCanonicalName());
                    currentDirectory = null;
                }

            } catch (@SuppressWarnings("unused") final Exception e) {
                // ignore
            }
        }
    }

    public static void processBatch(ArrayList<LFN> lfnsToDelete) {
        Set<LFN> processedToDelete = new HashSet<>();

        for (final LFN l : lfnsToDelete) {
            final List<LFN> members = LFNUtils.getArchiveMembers(LFNUtils.getRealLFN(l));

            print("Parsing lfn: " + l.getCanonicalName());
            if (members != null) {
                for (final LFN member : members) {
                    processedToDelete.add(member);
                    print("Found archive member: " + member.getCanonicalName());
                }
            }
            processedToDelete.add(l);
        }

        for (final LFN l : processedToDelete) {
            print("Removing lfn: " + l.getCanonicalName());
        }
    }

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
        }
    }

    private static void print(String message) {
        logger.log(Level.INFO, message);
    }

    public static void startCrawler() {
        final Collection<IndexTableEntry> indextableCollection = CatalogueUtils.getAllIndexTables();

        if (indextableCollection == null) {
            print("indextableCollection is null");
            return;
        }

        print("========== Directories iteration ==========");
        removeDirectories(indextableCollection);

        print("========== Files iteration ==========");
        removeFiles(indextableCollection);
    }
}