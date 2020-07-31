#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <memory.h>
#include "../rbf/rbfm.h"
#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator
const int MaxCatalogID = 3;
constexpr char CATALOG_TABLE[] = "Tables.tbl";
constexpr char CATALOG_COLUMN[] = "Columns.tbl";
constexpr char CATALOG_INDEX[] = "Indexs.tbl";

const std::string postfix = ".tbl";
const std::vector<std::string> CatalogColumnTupleNames{"table-id", "column-name", "column-type", "column-length", "column-position", "version"};
const std::vector<std::string> CatalogTableTupleNames{"table-id", "table-name", "file-name", "version"};
const std::vector<std::string> CatalogIndexTupleNames{"table-id", "column-position", "file-name"};

const std::vector<Attribute>& getCatalogTableAttribute();
const std::vector<Attribute>& getCatalogColumnAttribute();
const std::vector<Attribute>& getCatalogIndexAttribute();

inline std::string makeIndexFileName(const std::string& table, const std::string& attr){
    return table + "_" + attr + "_idx";
}
// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
    RBFM_ScanIterator _scanner;
public:
    RM_ScanIterator() = default;

    ~RM_ScanIterator() = default;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { return _scanner.getNextRecord(rid, data);};

    RC close() { return _scanner.close(); };

    void setScanner(RBFM_ScanIterator scanner){
        _scanner = scanner;
    }
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
    IX_ScanIterator _scanner;
public:
    RM_IndexScanIterator() {};    // Constructor
    ~RM_IndexScanIterator() {};    // Destructor

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key) { return _scanner.getNextEntry(rid, key); };    // Get next matching entry
    RC close() { return _scanner.close(); };                        // Terminate index scan
    void setScanner(IX_ScanIterator scanner){
        _scanner = scanner;
    }
};

// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC deleteTable(const std::string &tableName);

    RC getAttribute(int table_id, const std::string& attrName, Attribute& attr, int* posi=nullptr);
    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int version);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

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

    int getTableIdVersion(const std::string& tableName, int* version=nullptr, RID* rid=nullptr);

    RC mapRIDs(const std::string &tableName, const std::string &conditionAttribute, const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames, std::function<void(RID&, char*)> func);
    RC mapRIDs(const std::string &tableName, const std::string &conditionAttribute, const CompOp compOp, const void *value, const std::vector<std::string> &attributeNames, std::function<void(RID&, char*, FileHandle&)> func, FileHandle&);
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

protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                             // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    static RelationManager *_relation_manager;
};

#endif