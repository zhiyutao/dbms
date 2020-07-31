#include "pfm.h"
#include <unistd.h>
#include <iostream>
#include <memory.h>
#include <libgen.h>
#include <stack>

using namespace std;

PagedFileManager *PagedFileManager::_pf_manager = nullptr;

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() { delete _pf_manager; }

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

RC PagedFileManager::createFile(const std::string &fileName) {
    return wCreateFile(fileName);
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    return remove(fileName.c_str());
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    FILE *fPointer = NULL;
    fPointer = fopen(fileName.c_str(), "r+");
    if (fPointer == NULL) {
        return -1;
    } else {
        fileHandle.setFile(fPointer);
        return 0;
    }
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.releaseFile();
}

/* ================= FileHandle =============== */
FileHandle::SharedItem::~SharedItem() {
    if(name != NULL)
        fclose(name);
}
FileHandle::FileHandle() {}
FileHandle::~FileHandle(){}
// void FileHandle::_copyMembers(const FileHandle& fh){}
// FileHandle::FileHandle(const FileHandle& fh){ _copyMembers(fh);}
// FileHandle& FileHandle::operator = (const FileHandle& fh){ _copyMembers(fh); return *this;}

RC FileHandle::setFile(FILE *fp) {
    shared_item_ = std::make_shared<FileHandle::SharedItem>();
    shared_item_->name = fp;

    if (getSize() < PAGE_SIZE) {
        shared_item_->readPageCounter = 0;
        shared_item_->writePageCounter = 0;
        shared_item_->appendPageCounter = 0;
        if (ftruncate(fileno(shared_item_->name), PAGE_SIZE) < 0) {
            perror("setFile: ");
            exit(EXIT_FAILURE);
        }
        fseek(shared_item_->name, 0, SEEK_SET);
        unsigned counter[] = {0,0,0};
        fwrite(counter, sizeof(unsigned), 3, shared_item_->name);
        // fdatasync(fileno(name)); // remember to flush data !!
    } else {
        fseek(shared_item_->name, 0, SEEK_SET);
        char buf[3 * sizeof(unsigned)]{'\0'};
        if (fread(buf, sizeof(unsigned), 3, shared_item_->name) < 3) {
            perror("setFile: ");
            exit(EXIT_FAILURE);
        }
        shared_item_->readPageCounter = *(unsigned *) buf;
        shared_item_->writePageCounter = *(unsigned *) (buf + sizeof(unsigned));
        shared_item_->appendPageCounter = *(unsigned *) (buf + 2 * sizeof(unsigned));
    }
    return 0;
}

RC FileHandle::releaseFile() {
    shared_item_.reset();
    return 0;
}

int FileHandle::getSize() {
    fseek(shared_item_->name, 0, SEEK_END);
    return ftell(shared_item_->name);
}

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (!shared_item_) return -1;

    if (pageNum > getNumberOfPages() - 1) {
        return -1;
    }
    
    int rc = fseek(shared_item_->name, PAGE_SIZE * (pageNum + 1), SEEK_SET);
    int num = fread(data, sizeof(char), PAGE_SIZE, shared_item_->name);
    // cout << "[readPage] "<<getNumberOfPages()<<" " << num<< " "<<pageNum <<" "<< rc<<endl;

    if (num == PAGE_SIZE) {
        shared_item_->readPageCounter++;
        fseek(shared_item_->name, 0, SEEK_SET);
        fwrite(&(shared_item_->readPageCounter), sizeof(unsigned), 1, shared_item_->name);
        // fdatasync(fileno(name)); // remember to flush data !!
        return 0;
    }
    else {
        // perror("[readPage]");
        return -1;
    }
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (shared_item_->name == NULL) {
        return -1;
    }
    //    fseek(name, 0, SEEK_END);
    //    int size = ftell(name);  //判断文件大小
    if (pageNum > getNumberOfPages() - 1) {
        return -1;
    }

    fseek(shared_item_->name, PAGE_SIZE * (pageNum + 1), SEEK_SET);
    int num = fwrite(data, sizeof(char), PAGE_SIZE, shared_item_->name);
    if (num == PAGE_SIZE) {
        shared_item_->writePageCounter++;
        fseek(shared_item_->name, sizeof(unsigned), SEEK_SET);
        fwrite(&(shared_item_->writePageCounter), sizeof(unsigned), 1, shared_item_->name);
        // fdatasync(fileno(name)); // remember to flush data !!
        return 0;
    } else {
        return -1;
    }
}

RC FileHandle::appendPage(const void *data) {
    if (!shared_item_) return -1;
    fseek(shared_item_->name, (shared_item_->appendPageCounter + 1) * PAGE_SIZE, SEEK_SET);
    if (fwrite(data, sizeof(char), PAGE_SIZE, shared_item_->name) != PAGE_SIZE) return -1;
    shared_item_->appendPageCounter++;
    fseek(shared_item_->name, sizeof(unsigned) * 2, SEEK_SET);
    fwrite(&(shared_item_->appendPageCounter), sizeof(unsigned), 1, shared_item_->name);
    // fdatasync(fileno(name)); // remember to flush data !!
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return shared_item_->appendPageCounter;
}

int FileHandle::getTableID() {
    fseek(shared_item_->name, sizeof(unsigned) * 3, SEEK_SET);
    char buf[sizeof(int)]{'\0'};
    if (fread(buf, sizeof(int), 1, shared_item_->name) < 1) {
        perror("getTableID ");
        exit(EXIT_FAILURE);
    }
    return *(int *) buf;
}

void FileHandle::setTableID(int table_id) {
    fseek(shared_item_->name, sizeof(unsigned) * 3, SEEK_SET);
    fwrite(&table_id, sizeof(int), 1, shared_item_->name);
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = shared_item_->readPageCounter;
    writePageCount = shared_item_->writePageCounter;
    appendPageCount = shared_item_->appendPageCounter;
    return 0;
}

//* ================== Functions ================= */
bool is_file_exists(const char *path) {
    return access(path, F_OK) == 0 ? true : false;
}

RC wCreateFile(const std::string &fileName) {
    /*This method creates an empty-paged file called fileName.
    The file should not already exist. This method should not create any pages in the file.*/
    if (is_file_exists(fileName.c_str()))
        return -1;

    // make sure the directory of file exsits
    char *namebuf = new char[fileName.size() + 1]{'\0'};
    memcpy(namebuf, fileName.c_str(), fileName.size());
    char *dirn = dirname(namebuf);
    wMkdirs(dirn);
    delete[]namebuf;
    // create new file
    FILE *fp = fopen(fileName.c_str(), "a");
    if (fp == NULL) {
        perror("wCreateFile: ");
        exit(EXIT_FAILURE);
    }
    fclose(fp);
    return 0;
}

RC wRemoveFile(const std::string &fileName) {
    return remove(fileName.c_str());
}

void wMkdirs(char *pathname) {
    if (is_file_exists(pathname))
        return;

    std::string pathstr(pathname);
    char *namebuf = new char[pathstr.size() + 1]{'\0'};
    int ci;
    for (ci = pathstr.size() - 1; ci >= 0; --ci) {
        if (pathstr[ci] == '/') {
            memcpy(namebuf, pathstr.c_str(), ci);
            namebuf[ci] = '\0';
            if (is_file_exists(namebuf))
                break;
        }
    }
    namebuf[ci] = '/';
    for (ci = ci + 1; ci < pathstr.size(); ++ci) {
        if (pathstr[ci] == '/') {
            namebuf[ci] = '\0';
            if (mkdir(namebuf, S_IRWXG | S_IRWXO | S_IRWXU) != 0) {
                perror("wMkdirs: ");
                exit(EXIT_FAILURE);
            }
        }
        namebuf[ci] = pathstr[ci];
    }

    if (mkdir(namebuf, S_IRWXG | S_IRWXO | S_IRWXU) != 0) {
        perror("wMkdirs: ");
        exit(EXIT_FAILURE);
    }

}

