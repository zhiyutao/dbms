#ifndef _pfm_h_
#define _pfm_h_
#include <string>
#include <climits>
#include <sys/stat.h>
#include <sys/mman.h>
#include <memory>
typedef unsigned PageNum;
typedef int RC;
typedef unsigned char byte;

#define PAGE_SIZE 4096

RC wCreateFile(const std::string& fileName);
RC wRemoveFile(const std::string& fileName);
void wMkdirs(char* pathname);

class FileHandle;

class  PagedFileManager{

public:
    static PagedFileManager &instance();                                // Access to the _pf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new file
    RC destroyFile(const std::string &fileName);                        // Destroy a file
    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
    RC closeFile(FileHandle &fileHandle);                               // Close a file

protected:
    PagedFileManager();                                                 // Prevent construction
    ~PagedFileManager();                                                // Prevent unwanted destruction
    PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
    PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

private:
    static PagedFileManager *_pf_manager;
};

class FileHandle {
    // void _copyMembers(const FileHandle&);
public:
    struct SharedItem {
        // variables to keep the counter for each operation
        unsigned readPageCounter = 0;
        unsigned writePageCounter = 0;
        unsigned appendPageCounter = 0;
        FILE *name = NULL;                                                  // file pointer
        ~SharedItem();
    };
    std::shared_ptr<FileHandle::SharedItem> shared_item_;

    FileHandle();                                                       // Default constructor
    ~FileHandle();                                                      // Destructor
    // FileHandle(const FileHandle&);
    // FileHandle& operator = (const FileHandle&);

    RC setFile(FILE*);                                                    // set FIle* name and detect meta page
    RC releaseFile();                                                     // close FILE* name
    int getSize();                                                         // return file size
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();                                        // Get the number of pages in the file
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                            unsigned &appendPageCount);                 // Put current counter values into variables
    int getTableID();
    void setTableID(int table_id);
};

#endif


