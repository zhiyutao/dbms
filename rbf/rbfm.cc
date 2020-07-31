#include "rbfm.h"
#include <iostream>
#include <iomanip>
#include <memory.h>

void pushBackTo(std::vector<char> &data, const char *src, int len) {
    for (int i = 0; i < len; ++i) {
        data.push_back(src[i]);
    }
}

std::unordered_map<std::string, int> getFieldIndex(const std::vector<Attribute> &recordDescriptor) {
    std::unordered_map<std::string, int> res;
    for (int i = 0; i < recordDescriptor.size(); ++i) {
        res[recordDescriptor[i].name] = i;
    }
    return res;
}

static void compress(char *pagefrom, TypeOffset startOffSet, TypeOffset shift) {
    int midlen = startOffSet - DataPage::getDataStackTop(pagefrom);
    char midbuf[midlen];
    memcpy(midbuf, pagefrom + DataPage::getDataStackTop(pagefrom), midlen);
    memcpy(pagefrom + DataPage::getDataStackTop(pagefrom) + shift, midbuf, midlen);

    for (TypeOffset i = 0; i < DataPage::getSlotNum(pagefrom); ++i) {
        SlotItem &slotreftmp = *(SlotItem *) (pagefrom + PAGEHEADSIZE + i * sizeof(SlotItem));
        if (slotreftmp.offset < 0 && -slotreftmp.offset < startOffSet) { // indirected 
            slotreftmp.offset -= shift;
        }
        else if(slotreftmp.offset > 0 && slotreftmp.offset < startOffSet){ // directed
            slotreftmp.offset += shift;
        }
    }
}


static void serialize(std::vector<char> &dst, const char *src, SlotItem &slot,
                      const std::vector<Attribute> &recordDescriptor, char schema_version = 0) {
    /* serialize from caller format to disk format, and setting necessary value for slot */
    slot.field_num = recordDescriptor.size();
    int indicator_len = getIndicatorLen(slot.field_num);
    const char *data_start = src + indicator_len;
    // copy data part
    std::vector<TypeOffset> offsets{0};
    for (int i = 0; i < recordDescriptor.size(); ++i) {
        if (testBit(src, i)) {
            offsets.push_back(offsets.back());
            continue;
        }
        switch (recordDescriptor[i].type) {
            case TypeReal/* constant-expression */:
                pushBackTo(dst, data_start, 4);
                offsets.push_back(offsets.back() + 4);
                data_start = data_start + 4;
                break;
            case TypeInt:
                pushBackTo(dst, data_start, 4);
                offsets.push_back(offsets.back() + 4);
                data_start = data_start + 4;
                break;
            case TypeVarChar: {
                int varlen = *(int *) data_start;
                data_start = data_start + 4;
                pushBackTo(dst, data_start, varlen);
                data_start = data_start + varlen;
                offsets.push_back(offsets.back() + varlen);
                break;
            }
            default:
                printf("Undefined type! ");
                exit(EXIT_FAILURE);
        }
    }
    // prepare metadata part
    dst.emplace_back(); // slotnum
    dst.push_back(schema_version);
    for (int i = 0; i < offsets.size() - 1; ++i) {
        pushBackTo(dst, (char *) &offsets[i], sizeof(TypeOffset));
    }
    pushBackTo(dst, src, indicator_len); // null indicator
    slot.data_size = offsets.back();
    slot.metadata_size = dst.size() - slot.data_size;
    if (dst.size() < 9){     // for secondary record make every record size >= 9 bytes
        slot.metadata_size += 9 - dst.size();
        dst.resize(9);
        std::cout << "extend to 9"<<std::endl;
    }
}

static void deserializeField(std::vector<char> &dst, const char *src, const SlotItem &slot, const Attribute &recordDescriptor,
                 int field_idx) {
    /* deserialize from disk format to caller format, and push back into dst */
    TypeOffset read_offset =
            slot.data_size + sizeof(TypeSlotNum) + sizeof(TypeSchemaVersion) + sizeof(TypeOffset) * field_idx;
    read_offset = *(TypeOffset *) (src + read_offset);
    switch (recordDescriptor.type) {
        case TypeReal:
            pushBackTo(dst, src + read_offset, 4);
            break;
        case TypeInt:
            pushBackTo(dst, src + read_offset, 4);
            break;
        case TypeVarChar: {
            int varlen;
            if (field_idx == slot.field_num - 1)
                varlen = slot.data_size - read_offset;
            else {
                TypeOffset &endoffset = *(TypeOffset *) (src + slot.data_size + sizeof(TypeSlotNum) +
                                                         sizeof(TypeSchemaVersion) + OFFTSIZE * (field_idx + 1));
                varlen = endoffset - read_offset;
            }
            pushBackTo(dst, (char *) &varlen, 4);
            pushBackTo(dst, src + read_offset, varlen);
            break;
        }
        default:
            printf("Undefined type! ");
            exit(EXIT_FAILURE);
    }
}

static void deserialize(std::vector<char> &dst, const char *src, const SlotItem &slot,
                        const std::vector<Attribute> &recordDescriptor) {
    /* deserialize from disk format to caller format, and store result in dst */
    const char *metadata_start = src + slot.data_size;
    const char *indicator_start =
            metadata_start + sizeof(TypeSlotNum) + sizeof(TypeSchemaVersion) + OFFTSIZE * slot.field_num;
    int indicator_len = getIndicatorLen(slot.field_num);
    // copy null indicator
    for (int i = 0; i < indicator_len; ++i) {
        dst.push_back(indicator_start[i]);
    }
    // parse data part
    for (int i = 0; i < slot.field_num; ++i) {
        if (testBit(dst.data(), i)) continue;
        deserializeField(dst, src, slot, recordDescriptor[i], i);
    }
}

template <typename Num>
int compareNumber(Num a, Num b){
    if(a == b) return 0;
    else if(a < b) return -1;
    else return 1;
}

static bool applyComp(CompOp compOp, const char* left, const char* right, const Attribute& recordDescriptor){
    int res;
    if(recordDescriptor.type == TypeVarChar){
        int len1 = *(int*)left, len2 = *(int*)right;
        res = memcmp(left + sizeof(int), right+sizeof(int), std::min(len1, len2));
        if(res == 0 && len1 != len2){
            res = len1 < len2 ? -1 : 1;
        }
    }
    else if(recordDescriptor.type == TypeInt){
        int a = *(int*)left, b = *(int*)right;
        res = compareNumber(a, b);
    }
    else if(recordDescriptor.type == TypeReal){
        float a = *(float*)left, b = *(float*)right;
        res = compareNumber(a, b);
    }
    
    switch (compOp)
    {
        case EQ_OP:  return res == 0;
        case LT_OP: return res < 0;
        case LE_OP: return res <= 0;
        case GT_OP: return res > 0;
        case GE_OP: return res >= 0;
        case NE_OP: return res != 0;
        default: break;
    };
    return false;
}

/* ============ RBFM ============ */
RecordBasedFileManager *RecordBasedFileManager::_rbf_manager = nullptr;

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() { delete _rbf_manager; }

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return wCreateFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return wRemoveFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    FILE *fp = fopen(fileName.c_str(), "r+");
    if (fp == NULL) return -1;
    fileHandle.setFile(fp);
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return fileHandle.releaseFile();
}


RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid, char version) {
    // prepare data that will be written
    SlotItem slot;
    std::vector<char> databuf;
    serialize(databuf, (const char *) data, slot, recordDescriptor, version);
    // traversal all existing pages
    char *pagebuf = new char[PAGE_SIZE];
    int64_t pageidx = fileHandle.getNumberOfPages(); // read the last page
    DataPage datapage;
    for (pageidx = pageidx - 1; pageidx >= 0; --pageidx) {
        // reading page
        if (fileHandle.readPage(pageidx, pagebuf) < 0) {
            std::cerr << "insertRecord error !";
            exit(EXIT_FAILURE);
        }
        // check page availability
        datapage.reset(pagebuf);
        if (tryWriteToPage(datapage, databuf, slot, rid.slotNum)) {
            rid.pageNum = (unsigned) pageidx;
            fileHandle.writePage(pageidx, datapage.data());
            return 0;
        }
        pagebuf = datapage.release();
    }
    // new page
    DataPage::InitializePage(pagebuf);
    datapage.reset(pagebuf);
    if (tryWriteToPage(datapage, databuf, slot, rid.slotNum)) {
        rid.pageNum = fileHandle.getNumberOfPages();
        fileHandle.appendPage(datapage.data());
        return 0;
    }
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            // std::cerr << "readRecord error !";
            return -1;
        }
        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));
        //std::cout << "[readRecord] fieldnum: " << slotref.field_num << " data_size " << slotref.data_size
        //          << " meta_size " << slotref.metadata_size << " offset " << slotref.offset << std::endl;
        // check slot validity

        if (slotref.offset > 0) { // direct slot
            char *data_start = pagebuf + slotref.offset;
            std::vector<char> databuf;
            deserialize(databuf, data_start, slotref, recordDescriptor);
            memcpy(data, databuf.data(), databuf.size());
            return 0;
        } else if (slotref.offset < 0) { // indirect slot
            char *data_start = pagebuf - slotref.offset;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
            //std::cout << "[readRecord] pageNum: " << pageNum << " slotNum " << slotNum << std::endl;
        } else { // deleted slot
            return -1;
        }
    } while (1);
    return -1;
}

RC RecordBasedFileManager::readRawRecord(FileHandle& fileHandle, const RID& rid, char* databuf, int& from_version, SlotItem& slot){
    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            // std::cerr << "readRecord error !";
            return -1;
        }
        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));
        // check slot validity
        if (slotref.offset > 0) { // direct slot
            char *data_start = pagebuf + slotref.offset;
            slot = slotref;
            from_version = *(TypeSchemaVersion*)(data_start + slotref.data_size + sizeof(TypeSlotNum));
            memcpy(databuf, data_start, slot.data_size + slot.metadata_size);
            return 0;
        } else if (slotref.offset < 0) { // indirect slot
            char *data_start = pagebuf - slotref.offset;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
        } else { // deleted slot
            return -1;
        }
    } while (1);
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {

    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            return -1;
        }
        //std::cout<<"data stack top before delete: "<<DataPage::getDataStackTop(pagebuf)<<std::endl;

        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));

        if (slotref.offset > 0) { // direct slot
            compress(pagebuf, slotref.offset, slotref.data_size + slotref.metadata_size);
            slotref.offset = 0;
            TypeOffset dataStackTop = DataPage::getDataStackTop(pagebuf);
            dataStackTop += slotref.data_size + slotref.metadata_size;
            memcpy(pagebuf + sizeof(TypeOffset), &dataStackTop, sizeof(TypeOffset));
            fileHandle.writePage(pageNum, pagebuf);
            //std::cout<<"data stack top after delete: "<<DataPage::getDataStackTop(pagetmp)<<std::endl;
            return 0;
        } else if (slotref.offset < 0) {
            char *data_start = pagebuf - slotref.offset;
            auto oldPageNum = pageNum;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
            // delete current reference
            compress(pagebuf, -slotref.offset, 9);
            slotref.offset = 0;
            TypeOffset dataStackTop = DataPage::getDataStackTop(pagebuf);
            dataStackTop += 9;
            memcpy(pagebuf + sizeof(TypeOffset), &dataStackTop, sizeof(TypeOffset));
            fileHandle.writePage(oldPageNum, pagebuf);
            //   |type s m |p n|
        } else {
            return -1;
        }
    } while (1);
}


RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    int indicator_len = recordDescriptor.size() / CHAR_BIT + (recordDescriptor.size() % CHAR_BIT == 0 ? 0 : 1);
    char *dataidx = (char *) data + indicator_len;

    for (int i = 0; i < recordDescriptor.size(); ++i) {
        std::cout << recordDescriptor[i].name << ": ";
        if (testBit((char *) data, i)) {
            std::cout << std::setw(8) << std::left << "NULL ";
            continue;
        }
        switch (recordDescriptor[i].type) {
            case TypeInt:
                std::cout << std::setw(8) << std::left << *(int *) dataidx << " ";
                dataidx = dataidx + sizeof(int);
                break;
            case TypeReal:
                std::cout << std::setw(8) << std::left << *(float *) dataidx << " ";
                dataidx = dataidx + sizeof(float);
                break;
            case TypeVarChar: {
                int &varlen = *(int *) dataidx;
                dataidx = dataidx + sizeof(int);
                for (int j = 0; j < varlen; ++j) {
                    std::cout << *dataidx;
                    dataidx = dataidx + sizeof(char);
                }
                std::cout << " ";
                break;
            }
            default:
                break;
        }
    }
    std::cout << std::endl;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid, char version) {
    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            return -1;
        }
        //std::cout<<"data stack top before update : "<<DataPage::getDataStackTop(pagebuf)<<std::endl;
        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));

        if (slotref.offset > 0) { // direct slot
            SlotItem slot;
            std::vector<char> databuf;
            //serialize new data
            serialize(databuf, (const char *) data, slot, recordDescriptor, version);
            TypeOffset newLen = databuf.size();
            TypeOffset shift = slotref.data_size + slotref.metadata_size - newLen;

            //same space
            if (shift == 0) {
                //write new data
                memcpy(pagebuf + slotref.offset, databuf.data(), newLen);
                slot.offset = slotref.offset;
                memcpy(pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem), &slot, sizeof(slot));
            }
            else if (shift > 0) { //old record is bigger than new record => move to right
                compress(pagebuf, slotref.offset, shift);
                slot.offset = slotref.offset + shift;
                memcpy(pagebuf + slot.offset, databuf.data(), newLen);
                memcpy(pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem), &slot, sizeof(slot));
                TypeOffset dataStackTop = DataPage::getDataStackTop(pagebuf);
                dataStackTop += shift;
                memcpy(pagebuf + sizeof(TypeOffset), &dataStackTop, sizeof(TypeOffset));
            }              
            else {  //old record is smaller than new record => move to left
                //there are enough space in the same page
                if (DataPage::getEmptySize(pagebuf) + shift >= 0) {
                    compress(pagebuf, slotref.offset, shift);
                    slot.offset = slotref.offset + shift;
                    memcpy(pagebuf + slot.offset, databuf.data(), newLen);
                    memcpy(pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem), &slot, sizeof(slot));

                    TypeOffset dataStackTop = DataPage::getDataStackTop(pagebuf);
                    dataStackTop += shift;
                    memcpy(pagebuf + sizeof(TypeOffset), &dataStackTop, sizeof(TypeOffset));
                } else { //there are not enough space in the same page
                    shift = slotref.data_size + slotref.metadata_size - 9;
                    compress(pagebuf, slotref.offset, shift);
                    slot.offset = -(slotref.offset + shift);
                    memcpy(pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem), &slot, sizeof(slot));
                    pagebuf[-slot.offset] = (char) slotNum;
                    TypeOffset dataStackTop = DataPage::getDataStackTop(pagebuf);
                    dataStackTop += shift;
                    memcpy(pagebuf + sizeof(TypeOffset), &dataStackTop, sizeof(TypeOffset));

                    RID ridtmp;
                    insertRecord(fileHandle, recordDescriptor, data, ridtmp, version);
                    memcpy(pagebuf - slot.offset + sizeof(TypeSlotNum), &(ridtmp.pageNum), sizeof(ridtmp.pageNum));
                    memcpy(pagebuf - slot.offset + sizeof(TypeSlotNum) + sizeof(pageNum), &(ridtmp.slotNum), sizeof(ridtmp.slotNum));
                }
            }
            fileHandle.writePage(pageNum, pagebuf);
            return 0;
        } else if (slotref.offset < 0) {
            char *data_start = pagebuf - slotref.offset;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
        } else {
            return -1;
        }
    } while (1);

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    // find which field to read
    auto dict = getFieldIndex(recordDescriptor);
    if (dict.count(attributeName) == 0)
        return -1;
    int field_idx = dict[attributeName];
    // read page
    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            // std::cerr << "readRecord error !";
            return -1;
        }
        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));
        // std::cout << "[readRecord] fieldnum: "<<slotref.field_num << " data_size "<< slotref.data_size << " meta_size "<<slotref.metadata_size << " offset "<<slotref.offset<<std::endl;
        // check slot validity
        if (slotref.offset > 0) { // direct slot
            char *data_start = pagebuf + slotref.offset;
            std::vector<char> databuf;
            // copy null indicator
            int indicator_len = getIndicatorLen(slotref.field_num);
            int read_offset = slotref.data_size + sizeof(TypeSlotNum) + sizeof(TypeSchemaVersion) +
                              sizeof(TypeOffset) * slotref.field_num;
            pushBackTo(databuf, data_start + read_offset, indicator_len);
            if (testBit(databuf.data(), field_idx)) { // if this field is NULL
                memcpy(data, databuf.data(), databuf.size());
                return 0;
            }
            deserializeField(databuf, data_start, slotref, recordDescriptor[field_idx], field_idx);
            memcpy(data, databuf.data(), databuf.size());
            return 0;
        } else if (slotref.offset < 0) { // indirect slot
            char *data_start = pagebuf - slotref.offset;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
        } else { // deleted slot
            return -1;
        }
    } while (1);
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    auto field_dict = getFieldIndex(recordDescriptor);
    if (conditionAttribute.size() == 0 && compOp != NO_OP) // invalid condition
        return -1;
    if(conditionAttribute.size() != 0 && field_dict.count(conditionAttribute) == 0) // invalid conditionAttribute
        return -1; 
    for(const auto& s: attributeNames){
        if(field_dict.count(s) == 0)
            return -1;
    }
    rbfm_ScanIterator.setParams(fileHandle, recordDescriptor, conditionAttribute, 
        compOp, value, attributeNames, field_dict);
   return 0;
}
int RecordBasedFileManager::getVersion(const RID &rid, FileHandle &fileHandle) {
    char pagebuf[PAGE_SIZE];
    auto pageNum = rid.pageNum;
    auto slotNum = rid.slotNum;

    do {
        if (fileHandle.readPage(pageNum, pagebuf) != 0) {
            // std::cerr << "readRecord error !";
            return -1;
        }
        SlotItem &slotref = *(SlotItem *) (pagebuf + PAGEHEADSIZE + slotNum * sizeof(SlotItem));
        //std::cout << "[readRecord] fieldnum: " << slotref.field_num << " data_size " << slotref.data_size
        //          << " meta_size " << slotref.metadata_size << " offset " << slotref.offset << std::endl;
        // check slot validity

        if (slotref.offset > 0) { // direct slot
            char *metadata_start = pagebuf + slotref.offset + slotref.data_size;
            TypeSchemaVersion res = *(TypeSchemaVersion*)(metadata_start + sizeof(TypeSlotNum));
            return res;
        } else if (slotref.offset < 0) { // indirect slot
            char *data_start = pagebuf - slotref.offset;
            pageNum = *(unsigned *) (data_start + sizeof(TypeSlotNum));
            slotNum = *(unsigned *) (data_start + sizeof(TypeSlotNum) + sizeof(pageNum));
            //std::cout << "[readRecord] pageNum: " << pageNum << " slotNum " << slotNum << std::endl;
        } else { // deleted slot
            return -1;
        }
    } while (1);
    return -1;
}

bool RecordBasedFileManager::tryWriteToPage(DataPage &datapage, std::vector<char> &databuf, SlotItem &slot,
                                            unsigned int &ret_slotidx) {
    int empty_size = datapage.getEmptySize();
    // std::cout << "[tryWriteToPage] empty size: "<<empty_size<<std::endl;
    if (datapage.hasEmptySlot()) {
        int slotidx = datapage.findEmptySlot();
        if (empty_size >= databuf.size()) { // write to page
            databuf[slot.data_size] = (char) slotidx;
            TypeOffset new_stack_top = datapage.writeData(databuf.data(), databuf.size());
            slot.offset = new_stack_top;
            datapage.writeSlot(slotidx, slot);
            ret_slotidx = (unsigned) slotidx;
            return true;
        }
    } else if (empty_size >= databuf.size() + sizeof(SlotItem)) {
        int slotidx = datapage.appendSlot();
        databuf[slot.data_size] = (char) slotidx;
        TypeOffset new_stack_top = datapage.writeData(databuf.data(), databuf.size());
        slot.offset = new_stack_top;
        datapage.writeSlot(slotidx, slot);
        ret_slotidx = (unsigned) slotidx;
        return true;
    }
    return false;
}

void RecordBasedFileManager::convertVersion(char* from_data, char* to_data, std::vector<Attribute>& from_attrs, std::vector<Attribute>& to_attrs, const SlotItem& slot){
    int indicator_len = getIndicatorLen(to_attrs.size());
    auto name2idx = getFieldIndex(from_attrs);
    auto indicator_start = from_data+slot.data_size + sizeof(TypeSchemaVersion) + sizeof(TypeSlotNum) + slot.field_num * sizeof(TypeOffset);
    std::vector<char> databuf;
    databuf.resize(indicator_len, 0);

    for(int i = 0; i < to_attrs.size(); ++ i){
        if(name2idx.count(to_attrs[i].name) == 0){ // isnull
            setBit(databuf.data(), i);
            continue;
        }
        int field_idx = name2idx[to_attrs[i].name];
        if(testBit(indicator_start, field_idx)){ // is null
            setBit(databuf.data(), i);
            continue;
        }
        deserializeField(databuf, from_data, slot, from_attrs[ name2idx[to_attrs[i].name] ], name2idx[to_attrs[i].name]);
    }
    memcpy(to_data, databuf.data(), databuf.size());
}

/* =========== ScanIterator ========== */
void RBFM_ScanIterator::_copyMembers(const RBFM_ScanIterator& other){
    next_rid = other.next_rid;
    _conditionAttribute = other._conditionAttribute;
    _compOp = other._compOp;
    // memcpy(pagebuf, other.pagebuf, PAGE_SIZE); // don't need to copy because we won't reuse this data
    _field_dict = other._field_dict;
    _fh = other._fh;
    _recordDescriptor = other._recordDescriptor;
    _valuelen = other._valuelen;
    _attributeNames = other._attributeNames;
   _value = new char[_valuelen];
   memcpy(_value, other._value, _valuelen);
}
RBFM_ScanIterator::RBFM_ScanIterator(const RBFM_ScanIterator& other){ _copyMembers(other); }
RBFM_ScanIterator& RBFM_ScanIterator::operator = (const RBFM_ScanIterator& other){ _copyMembers(other); return *this; }
RBFM_ScanIterator::~RBFM_ScanIterator(){ close(); }
RC RBFM_ScanIterator::close() { 
    delete [] _value;
    _value = nullptr;
    return 0; 
}

void RBFM_ScanIterator::setParams(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                            const std::string &conditionAttribute, const CompOp compOp, const void *value,
                            const std::vector<std::string> &attributeNames, const std::unordered_map<std::string, int>& field_dict){
    next_rid.pageNum = 0;
    next_rid.slotNum = 0;

    _conditionAttribute = conditionAttribute;
    _compOp = compOp;
    _fh = fileHandle;
    _recordDescriptor = recordDescriptor;
    _attributeNames = attributeNames;
    _field_dict = field_dict;
    if(value == NULL){
        _value = nullptr;
        return;
    }
    switch (recordDescriptor[_field_dict[conditionAttribute]].type)
    {
    case TypeVarChar:
    {
        int varlen = *(int*)value;
        _value = new char[4 + varlen];
        _valuelen = 4 + varlen;
        memcpy(_value, value, 4 + varlen);
        break;
    }
    default:
        _value = new char[4];
        _valuelen = 4;
        memcpy(_value, value, 4);
        break;
    }
}
RC  RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 
    ScanCODE errcode;
    while((errcode = _getNextRecord(data)) != ScanCODE::OVERPAGE){
        if(errcode == ScanCODE::SUCC){
            rid.pageNum = next_rid.pageNum;
            rid.slotNum = next_rid.slotNum;
            next_rid.slotNum ++;
            return 0;
        }
        else if(errcode == ScanCODE::OVERSLOT){
            next_rid.pageNum ++;
            next_rid.slotNum = 0;
        }
        else if(errcode == ScanCODE::INVALID_RECORD){
            next_rid.slotNum ++;
        }
        else
        {
            std::cerr << "Invalid ScanCODE" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    return RBFM_EOF;
}
ScanCODE RBFM_ScanIterator::_getNextRecord(void *data){
    // std::cout << "[scan] page count "<< _fh.getNumberOfPages() << std::endl;
    if(_fh.readPage(next_rid.pageNum, pagebuf) != 0)
        return ScanCODE::OVERPAGE;
    // std::cout << "[scan ] slot len "<<DataPage::getSlotTableLen(pagebuf) << std::endl;
    if(DataPage::getSlotTableLen(pagebuf)  <= next_rid.slotNum * sizeof(SlotItem))
        return ScanCODE::OVERSLOT;

    SlotItem& slotref = *(SlotItem*)(pagebuf + PAGEHEADSIZE + next_rid.slotNum * sizeof(SlotItem));
    if(slotref.offset <= 0)
        return ScanCODE::INVALID_RECORD;
    
    char* data_start = pagebuf + slotref.offset;
    int indicator_len = getIndicatorLen(slotref.field_num);
    char* indicator_start = data_start + slotref.data_size + sizeof(TypeSlotNum) + sizeof(TypeSchemaVersion) + sizeof(TypeOffset) * slotref.field_num;
    std::vector<char> databuf;
    if(_compOp != NO_OP){ // check comparision condition
        if (testBit(indicator_start, _field_dict[_conditionAttribute])){ // isNULL
            return ScanCODE::INVALID_RECORD;
        }
        else {
            auto field_idx = _field_dict[_conditionAttribute];
            deserializeField(databuf, data_start, slotref, _recordDescriptor[field_idx], field_idx);
            if(!applyComp(_compOp, databuf.data(), _value, _recordDescriptor[field_idx]))
                return ScanCODE::INVALID_RECORD;
        }
    }
    // pass check -> serialize fields
    databuf.clear();
    indicator_len = getIndicatorLen(_attributeNames.size());
    databuf.resize(indicator_len, 0);
    for(int i = 0; i < _attributeNames.size(); ++ i){
        auto field_idx = _field_dict[_attributeNames[i]];
        if(testBit(indicator_start, field_idx)){
            setBit(databuf.data(), i);
            continue;
        }
        deserializeField(databuf, data_start, slotref, _recordDescriptor[field_idx], field_idx);
    }
    
    memcpy(data, databuf.data(), databuf.size());
    return ScanCODE::SUCC;
}
/* =========== DataPage ============ */
void DataPage::InitializePage(char *page) {
    constexpr TypeOffset slot_table_len_init = 0, data_stack_top_init = PAGE_SIZE;
    bzero(page, PAGE_SIZE);
    memcpy(page, &slot_table_len_init, sizeof(TypeOffset));
    memcpy(page + sizeof(TypeOffset), &data_stack_top_init, sizeof(TypeOffset));
    // std::cout<< "[InitializePage] here" <<std::endl;
}

void DataPage::_parsePage() {
    slot_table_len_ = (TypeOffset *) raw_page_;
    data_stack_top_ = (TypeOffset *) (raw_page_ + sizeof(TypeOffset));
    // set empty slot counter
    int counter = 0;
    char *slot_start = raw_page_ + PAGEHEADSIZE;
    for (int i = 0; i < *slot_table_len_; i += sizeof(SlotItem)) {
        TypeOffset &offset = *(TypeOffset *) slot_start;
        if (offset == 0)
            counter++;
        slot_start = slot_start + sizeof(SlotItem);
    }
    empty_slot_counter_ = counter;
    // std::cout << "[_parsePage] slot_table_len_: "<<*slot_table_len_ << " data_stack_top_: " << *data_stack_top_ << " empty_slot_counter_: "<< empty_slot_counter_<< std::endl;
}

void DataPage::reset(char *page) {
    if (raw_page_ != NULL)
        delete[]raw_page_;
    raw_page_ = page;
    page = NULL;
    _parsePage();
}

char *DataPage::release() {
    char *res = raw_page_;
    raw_page_ = NULL;
    _restoreStatus();
    return res;
}

TypeOffset DataPage::writeData(const char *data, int size) {
    *data_stack_top_ -= size;
    memcpy(raw_page_ + *data_stack_top_, data, size);
    return *data_stack_top_;
}

TypeOffset DataPage::writeSlot(int slotidx, const SlotItem &slot) {
    memcpy(raw_page_ + PAGEHEADSIZE + slotidx * sizeof(SlotItem), &slot, sizeof(slot));
    return slotidx;
}

int DataPage::findEmptySlot() {
    char *slot_start = raw_page_ + PAGEHEADSIZE;
    for (int i = 0; i < *slot_table_len_; i += sizeof(SlotItem)) {
        TypeOffset &offset = *(TypeOffset *) slot_start;
        if (offset == 0)
            return i / sizeof(SlotItem);
        slot_start = slot_start + sizeof(SlotItem);
    }
    return -1;
}

int DataPage::appendSlot() {
    *slot_table_len_ += sizeof(SlotItem);
    return *slot_table_len_ / sizeof(SlotItem) - 1;
}
