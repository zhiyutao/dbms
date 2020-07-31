#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
#define NONLEAF -2
struct IndexPage;

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {
public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class IXFileHandle: public FileHandle {
    
public:
    // variables to keep counter for each operation
    // unsigned ixReadPageCounter;
    // unsigned ixWritePageCounter;
    // unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    // RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    RC setFile(FILE*);
    int getRootPageNum();
    int setRootPageNum(int);
    int getIdlePageNum();
    int setIdlePageNum(int);

    bool isOpen();
};

struct IndexItem { // composite Index
    int leftChildPageNum;
    std::vector<char> value;
    RID rid;
};
typedef std::function<int(const IndexItem&, const IndexItem&)> CompFunc;

class IX_ScanIterator {
public:

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    void setParams(IXFileHandle& ixFileHandle, const Attribute& attribute, const void* lowKey, const void* highKey, bool lowKeyInclusive, bool highKeyInclusive);
    RC init();
private:
    IXFileHandle fh_;
    Attribute attr_;
    IndexItem lowKey_, highKey_;
    bool lowKeyInclusive_, highKeyInclusive_, lowKeyNull_, highKeyNull_, inited_;
    int currPageNum_, currSlotNum_, loadedPageNum_; // internal state: currPageNum_ have to be a leaf node

    CompFunc comp_;
    char pagebuf_[PAGE_SIZE]; // single-thread

    ScanCODE _getNextEntry(RID&, void*, int&);
};
struct IndexPage {
    static const int PAGEHEADSIZE = 9; // slottablelen 2 + stack top 2 + nextleafid 4 + nextIdleId 1
    // statistics
    static void InitializePage(char* page);
    static void setSlotTableLen(char* page, TypeOffset);
    static void setStackTop(char* page, TypeOffset);
    static void setNextLeafId(char* page, int);
    static TypeOffset getSlotTableLen(char* page);
    static TypeOffset getStackTop(char* page);
    static int getNextLeafId(char* page);
    static TypeOffset getEmptySize(char *page);

    static void setEmptySlotFlag(char* page, bool);
    static bool hasEmptySlot(char* page);
    static int getSlotCount(char* page);
    // index operations
    static std::vector<char> readRawIndex(char* page, int i);   // not include leftChildPageNum
    static IndexItem readIndexItem(char* page, int i);  // not include leftChildPageNum
    static void insertValueTo(char* page, int childPageNum,std::vector<char>& value, int i);
    // insert to i-th index than split data into two page
    static void insertValueAndSplitPage(char* oldpage, char* newpage, int childPageNum, std::vector<char>& value, int i);
    static void appendTailChildPointer(char* page);
    static void setChildPageNum(char* page, int childPageNum, int i);
    static TypeOffset writeSlot(char* page, int slotidx, const SlotItem& slot);
    static TypeOffset writeData(char* page, TypeOffset offset, const char* data, int size);
    static TypeOffset appendData(char* page,const char* data, int size);
    static int appendSlot(char* page);

    static void deleteIndexFrom(char* page, int i);
    static void garbageSlotCollection(char* page);
    // data movement
    static void compress(char* page, TypeOffset start, TypeOffset end, TypeOffset offset);
};
#endif
