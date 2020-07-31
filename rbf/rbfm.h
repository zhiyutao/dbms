#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <sys/types.h>
#include <functional>
#include <unordered_map>
#include <iostream>
#include "pfm.h"
// definitions
#define OFFTSIZE 2 // 2bytes:2 ** 16 - 1 or 2 ** 15 - 1
typedef int16_t TypeOffset;
typedef uint8_t TypeSlotNum;
typedef uint8_t TypeSchemaVersion;
constexpr int PAGEHEADSIZE = 4; // slot_table_len_, data_stack_top_
struct SlotItem {
    TypeOffset offset;
    TypeOffset data_size;
    TypeOffset metadata_size;
    TypeOffset field_num;
};

class DataPage;

// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
} AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;     // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;

inline bool testBit(const char *buffer, int i) {
    int bitidx = CHAR_BIT - 1 - i % CHAR_BIT;
    return (buffer[i / CHAR_BIT] & (1 << bitidx)) != 0;
}

inline void setBit(char *buffer, int i) {
    int bitidx = CHAR_BIT - 1 - i % CHAR_BIT;
    buffer[i / CHAR_BIT] |= (1 << bitidx);
}

inline int getIndicatorLen(TypeOffset field_num) {
    return field_num / CHAR_BIT + (field_num % CHAR_BIT == 0 ? 0 : 1);
}

void pushBackTo(std::vector<char> &data, const char *src, int len);
std::unordered_map<std::string, int> getFieldIndex(const std::vector<Attribute> &recordDescriptor); // return attrName is which field

/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();
enum class ScanCODE {
    OVERPAGE = -1,
    OVERSLOT = -2,
    INVALID_RECORD = -3,
    SUCC = 0
};

class RBFM_ScanIterator {
public:
    RBFM_ScanIterator() = default;;
    RBFM_ScanIterator(const RBFM_ScanIterator&);
    RBFM_ScanIterator& operator = (const RBFM_ScanIterator&);
    ~RBFM_ScanIterator();

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &rid, void *data);
    RC close();

    void setParams(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                   const std::string &conditionAttribute, const CompOp compOp, const void *value,
                   const std::vector<std::string> &attributeNames,
                   const std::unordered_map<std::string, int> &field_dict);

    RID next_rid; // the beginning rid next time reading
private:
    ScanCODE _getNextRecord(void* data);
    void _copyMembers(const RBFM_ScanIterator&);
    
    std::string _conditionAttribute;
    CompOp _compOp;
    char pagebuf[PAGE_SIZE];
    std::unordered_map<std::string, int> _field_dict;
    FileHandle _fh;
    std::vector<Attribute> _recordDescriptor;
    char* _value = nullptr; // be careful
    int _valuelen = 0;
    std::vector<std::string> _attributeNames;
};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid,
                    char version);

    RC
    insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
        return insertRecord(fileHandle, recordDescriptor, data, rid, 0);
    }

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);
    RC readRawRecord(FileHandle& file, const RID& rid, char* databuf, int& from_version, SlotItem& slot);
    
    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid, char version);

    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid) {
        return updateRecord(fileHandle, recordDescriptor, data, rid, 0);
    }

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    int getVersion(const RID &rid, FileHandle &fileHandle);
    void convertVersion(char* from_data, char* to_data, std::vector<Attribute>& from_attrs, std::vector<Attribute>& to_attrs, const SlotItem&);
protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    bool tryWriteToPage(DataPage &datapage, std::vector<char> &databuf, SlotItem &slot, unsigned int &ret_slotidx);
    // check if can write databuf and slot into datapage, if succeed, return true and set ret_slotidx
private:
    static RecordBasedFileManager *_rbf_manager;
};

class DataPage {
private:
    TypeOffset *slot_table_len_;
    TypeOffset *data_stack_top_;
    char *raw_page_ = NULL;
    int empty_slot_counter_;

    void _parsePage();

    void _restoreStatus() {
        slot_table_len_ = NULL;
        data_stack_top_ = NULL;
        empty_slot_counter_ = 0;
    }

public:
    ~DataPage() {
        delete[] raw_page_;
    }

    static void InitializePage(char *pageptr);

    void reset(char *pageptr); // take charge of pageptr
    char *release();

    const char *const data() const {
        return raw_page_;
    }

    bool hasEmptySlot() {
        return empty_slot_counter_ > 0;
    }

    int findEmptySlot();

    int appendSlot();

    int getEmptySize() {
        return *data_stack_top_ - PAGEHEADSIZE - *slot_table_len_;
        // PAGE_SIZE - PAGEHEADSIZE - slot_table_len_ - (PAGE_SIZE - data_stack_top_ + 1)
    }

    TypeOffset writeData(const char *data, int size);

    TypeOffset writeSlot(int slotidx, const SlotItem &);

    static TypeOffset getSlotTableLen(char *page) {
        return *(TypeOffset *) page;
    }

    static TypeOffset getSlotNum(char *page) {
        return *(TypeOffset *) page / sizeof(SlotItem);
    }

    static TypeOffset getDataStackLen(char *page) {
        return PAGE_SIZE - *(TypeOffset *) (page + sizeof(TypeOffset));
    }

    static TypeOffset getDataStackTop(char *page) {
        return *(TypeOffset *) (page + sizeof(TypeOffset));
    }

    static TypeOffset getEmptySize(char *page) {
        return getDataStackTop(page) - PAGEHEADSIZE - getSlotTableLen(page);
    }
};

#endif
