#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>

#include "src/include/rbfm.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RM_ScanIterator();

        ~RM_ScanIterator();

        FileHandle *fileHandle;
        std::string table_name; //table name is the file name, we will need it for efficiency
        bool file_is_open = false;
        bool reset_values = true; //I can't belive I actually need this. Theres a stupid error that i'm not going to explain here

        void *pageData = malloc(4096);
        int end_of_page = 0;
        bool pageData_collected = false;

        //for the scan function we will want a RBFM_ScanIterator conditions and
        //the attributes we care about
        RecordBasedFileManager *rbfm; //we need a pointer to this because we are required to use its functions
//        RBFM_ScanIterator * rbfm_ScanIterator;
        std::vector<Attribute> attrs;
        std::vector<std::string> attributeNames;

        //---------
        std::vector<RID> rids;
        int index = 0;
        int current_page = -1; //set to -1 to trigger the read in the custom rbfm function

        //we also want a variable to determine which scan functinality we are using
        //if we are doing a catalog scan we can be simple and scan every entry else we need to scan
        //from the rids vector
        bool scan_function = false;

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    public:
        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    private:
        //TODO: move these to localized functions
        //as these are the master tables we NEED to keep track of their entries. We could scan but a scan is too complicated
        std::unordered_map<std::string, std::vector<RID>> tableRIDS;
        std::unordered_map<std::string, std::vector<RID>> columnRIDS;

        //NOT THIS ONE!
        std::vector<Attribute> table_attrs;
        std::vector<Attribute> column_attrs;
        RecordBasedFileManager *rbfm;
        bool has_catalog = false;
        unsigned tablecount = 0;

        std::unordered_map<AttrType, int> attrConvert{{AttrType::TypeInt, 0},
                                                      {AttrType::TypeReal, 1},
                                                      {AttrType::TypeVarChar, 2}};

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_