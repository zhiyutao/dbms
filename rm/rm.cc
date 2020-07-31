#include "rm.h"

const std::vector<Attribute> &getCatalogTableAttribute() {
    static std::vector<Attribute> res;
    if (res.size() == 4)
        return res;
    res.clear();
    res.emplace_back(Attribute{"table-id", TypeInt, 4});
    res.emplace_back(Attribute{"table-name", TypeVarChar, 50});
    res.emplace_back(Attribute{"file-name", TypeVarChar, 50});
    res.emplace_back(Attribute{"version", TypeInt, 4});
    return res;
}

const std::vector<Attribute> &getCatalogColumnAttribute() {
    static std::vector<Attribute> res;
    if (res.size() == 6)
        return res;
    res.clear();
    res.emplace_back(Attribute{"table-id", TypeInt, 4});
    res.emplace_back(Attribute{"column-name", TypeVarChar, 50});
    res.emplace_back(Attribute{"column-type", TypeInt, 4});
    res.emplace_back(Attribute{"column-length", TypeInt, 4});
    res.emplace_back(Attribute{"column-position", TypeInt, 4});
    res.emplace_back(Attribute{"version", TypeInt, 4});
    return res;
}

const std::vector<Attribute> &getCatalogIndexAttribute() {
    static std::vector<Attribute> res;
    if (res.size() == 3)
        return res;
    res.clear();
    res.emplace_back(Attribute{"table-id", TypeInt, 4});
    res.emplace_back(Attribute{"column-position", TypeInt, 4});
    res.emplace_back(Attribute{"file-name", TypeVarChar, 120});
    return res;
}

void prepareCatalogTableData(std::vector<char> &databuf, const std::vector<Attribute> &attrs, int table_id,
                             std::string table_name, std::string file_name, int version) {
    databuf.resize(getIndicatorLen(attrs.size()), 0);
    pushBackTo(databuf, (const char *) &table_id, 4);
    int varlen = table_name.size();
    pushBackTo(databuf, (const char *) &varlen, 4);
    pushBackTo(databuf, table_name.data(), varlen);
    varlen = file_name.size();
    pushBackTo(databuf, (const char *) &varlen, 4);
    pushBackTo(databuf, file_name.data(), varlen);
    pushBackTo(databuf, (const char *) &version, 4);
}

void prepareCatalogColumnData(std::vector<char> &databuf, const std::vector<Attribute> &recordDescriptor, int table_id,
                              const Attribute &attr, int column_position, int version) {
    databuf.resize(getIndicatorLen(recordDescriptor.size()), 0);
    pushBackTo(databuf, (const char *) &table_id, 4); // table_id
    int varlen = attr.name.size();
    pushBackTo(databuf, (const char *) &varlen, 4); // column-name
    pushBackTo(databuf, attr.name.data(), varlen);
    varlen = (int) attr.type;
    pushBackTo(databuf, (const char *) &varlen, 4); // column-type
    pushBackTo(databuf, (const char *) &(attr.length), 4); // column-length
    pushBackTo(databuf, (const char *) &column_position, 4); // column-position
    pushBackTo(databuf, (const char *) &version, 4);
}

void prepareCatalogIndexData(std::vector<char> &databuf, const std::vector<Attribute> &recordDescriptor, int table_id,
                                int column_position,  std::string file_name) {
    databuf.resize(getIndicatorLen(recordDescriptor.size()), 0);
    pushBackTo(databuf, (const char *) &table_id, 4);
    pushBackTo(databuf, (const char *) &column_position, 4);
    int varlen = file_name.size();
    pushBackTo(databuf, (const char *) &varlen, 4);
    pushBackTo(databuf, file_name.data(), varlen);
}

Attribute catalogColumnToAttribute(const char *data, int *pos = nullptr, int *version = nullptr) {
    Attribute res;
    int indicator_len = getIndicatorLen(6), offset = indicator_len + 4;
    // name
    int varlen = *(int *) (data + offset);
    offset += 4;
    res.name = std::string(data + offset, data + offset + varlen);
    offset += varlen;
    // type
    res.type = static_cast<AttrType>(*(int *) (data + offset));
    offset += 4;
    // length
    res.length = *(AttrLength *) (data + offset);
    offset += sizeof(AttrLength);
    // posi
    if (pos != nullptr)
        *pos = *(int *) (data + offset);
    offset += 4;
    // version
    if (version != nullptr)
        *version = *(int *) (data + offset);
    return res;
}

std::vector<char> stringToClientData(const std::string &s) {
    std::vector<char> res;
    int varlen = s.size();
    pushBackTo(res, (const char *) &varlen, 4);
    pushBackTo(res, s.data(), varlen);
    return res;
}

/* ================= RelationManager ================ */
RelationManager *RelationManager::_relation_manager = nullptr;

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() = default;

RelationManager::~RelationManager() { delete _relation_manager; }

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

RC RelationManager::createCatalog() {
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.createFile(CATALOG_TABLE) < 0)
        return -1;
    if (rbfm.createFile(CATALOG_COLUMN) < 0)
        return -1;
    if(rbfm.createFile(CATALOG_INDEX) < 0)
        return -1;
    
    // make catalog data
    FileHandle file;
    std::vector<char> databuf;
    RID rid;
    if (rbfm.openFile(CATALOG_TABLE, file) < 0)
        return -1;
    // insert to Tables
    prepareCatalogTableData(databuf, getCatalogTableAttribute(), 1, "Tables", CATALOG_TABLE, 0);
    rbfm.insertRecord(file, getCatalogTableAttribute(), databuf.data(), rid);
    prepareCatalogTableData(databuf, getCatalogColumnAttribute(), 2, "Columns", CATALOG_COLUMN, 0);
    rbfm.insertRecord(file, getCatalogTableAttribute(), databuf.data(), rid);
    prepareCatalogTableData(databuf, getCatalogIndexAttribute(), 3, "Indexs", CATALOG_INDEX, 0);
    rbfm.insertRecord(file, getCatalogTableAttribute(), databuf.data(), rid);
    file.setTableID(3);
    // insert to Columns
    if (rbfm.openFile(CATALOG_COLUMN, file) < 0)
        return -1;
    auto recordDescriptor = getCatalogColumnAttribute();
    auto attrs = getCatalogTableAttribute();
    for (int i = 0; i < attrs.size(); ++i) { // Tables column
        prepareCatalogColumnData(databuf, recordDescriptor, 1, attrs[i], i + 1, 0);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    for (int i = 0; i < recordDescriptor.size(); ++i) { // Columns column
        prepareCatalogColumnData(databuf, recordDescriptor, 2, recordDescriptor[i], i + 1, 0);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    attrs = getCatalogIndexAttribute();
    for (int i = 0; i < attrs.size(); ++i) { // Indexs Columns
        prepareCatalogColumnData(databuf, recordDescriptor, 3, attrs[i], i + 1, 0);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    return 0;
}

RC RelationManager::deleteCatalog() {
    if (RecordBasedFileManager::instance().destroyFile(CATALOG_COLUMN) < 0)
        return -1;
    if (RecordBasedFileManager::instance().destroyFile(CATALOG_TABLE) < 0)
        return -1;
    if (RecordBasedFileManager::instance().destroyFile(CATALOG_INDEX) < 0)
        return -1;
    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.createFile(tableName+postfix) < 0) {
        return -1;
    }

    FileHandle file;
    //modify catlog table
    if (rbfm.openFile(CATALOG_TABLE, file) < 0) {
        return -1;
    }
    RID rid;
    std::vector<char> databuf;
    int table_id = file.getTableID() + 1;
    auto recordDescriptor = getCatalogTableAttribute();
    prepareCatalogTableData(databuf, recordDescriptor, table_id, tableName, tableName + postfix, 0);
    rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    file.setTableID(table_id);

    //modify catlog column
    if (rbfm.openFile(CATALOG_COLUMN, file) < 0) {
        return -1;
    }
    recordDescriptor = getCatalogColumnAttribute();
    for (int i = 0; i < attrs.size(); ++i) {
        prepareCatalogColumnData(databuf, recordDescriptor, table_id, attrs[i], i + 1, 0);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    
    auto &rbfm = RecordBasedFileManager::instance();
    FileHandle file;
    std::vector<Attribute> recordDescriptor;
    if(getAttributes(tableName,recordDescriptor) < 0)
        return -1;

    // check for system table
    int table_id = getTableIdVersion(tableName);
    if(table_id  <= MaxCatalogID) return -1;
    if (rbfm.openFile(CATALOG_COLUMN,file)<0)
    {
        return -1;
    }
    
    mapRIDs("Columns", "table-id", EQ_OP, &table_id, {"table-id"}, [](RID &rid, char* data, FileHandle& fh){
        RecordBasedFileManager::instance().deleteRecord(fh, getCatalogColumnAttribute(), rid);
    }, file);

    if (rbfm.openFile(CATALOG_TABLE,file)<0)
    {
        return -1;
    }
    
    mapRIDs("Tables", "table-id", EQ_OP, &table_id, {"table-id"}, [](RID &rid, char* data, FileHandle& fh){
        RecordBasedFileManager::instance().deleteRecord(fh, getCatalogTableAttribute(), rid);
    }, file);

    rbfm.destroyFile(tableName + postfix);

    return 0;
}

RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
    auto &ix = IndexManager::instance();
    std::string file_name = makeIndexFileName(tableName, attributeName);
    if (ix.createFile(file_name) < 0) {
        return -1;
    }
    // find tableid and column position
    int table_id = getTableIdVersion(tableName);
    if(table_id < 0) return -1;
    Attribute attr; int position;
    if(getAttribute(table_id, attributeName, attr, &position) < 0) return -1;
    // insert into catalog Table
    FileHandle file;
    std::vector<char> indexbuf;
    auto& rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(CATALOG_INDEX, file) < 0) {
        return -1;
    }
    auto recordDescriptor = getCatalogIndexAttribute();
    prepareCatalogIndexData(indexbuf, recordDescriptor, table_id, position, file_name);
    RID rid;
    rbfm.insertRecord(file, recordDescriptor, indexbuf.data(), rid);
    // insert index into index file
    IXFileHandle ixfile;
    if(ix.openFile(file_name, ixfile) < 0) return -1;
    mapRIDs(tableName, "", NO_OP, NULL, {attributeName}, [&attr](RID& rid, char* data, FileHandle& fh){
        IXFileHandle *ixfile = (IXFileHandle*)&fh;
        auto& ix = IndexManager::instance();
        // skip null indicator
        ix.insertEntry(*ixfile, attr, data+1, rid);
    }, ixfile);
    return 0;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    std::string file_name = makeIndexFileName(tableName, attributeName);
    if(IndexManager::instance().destroyFile(file_name) < 0)
        return -1;

    std::vector<char> caller_format;
    int varlen = file_name.size();
    pushBackTo(caller_format, (char*)&varlen, 4); pushBackTo(caller_format, file_name.data(), varlen);
    FileHandle fh;
    if(RecordBasedFileManager::instance().openFile(CATALOG_INDEX, fh) < 0)
        return -1;

    mapRIDs("Indexs", "file-name", EQ_OP, caller_format.data(), {"table-id"}, [](RID &rid, char* data, FileHandle& fh){
        RecordBasedFileManager::instance().deleteRecord(fh, getCatalogIndexAttribute(), rid);
    }, fh);
    
    return 0;
}

int RelationManager::getTableIdVersion(const std::string &tableName, int *version, RID* rt_rid) {
    RM_ScanIterator scanner;
    if (scan("Tables", "table-name", EQ_OP, stringToClientData(tableName).data(), {"table-id", "version"}, scanner) < 0)
        return -1;
    RID rid;
    char databuf[9]; // 1 + 4 + 4

    if (scanner.getNextTuple(rid, databuf) == RM_EOF)
        return -1;
    scanner.close();
    if (version != nullptr)
        *version = *(int *) (databuf + 1 + sizeof(int));
    if(rt_rid != nullptr)
        *rt_rid = rid;
    return *(int *) (databuf + 1);
}

RC RelationManager::mapRIDs(const std::string &tableName, const std::string &conditionAttribute, const CompOp compOp,
                            const void *value,
                            const std::vector<std::string> &attributeNames, std::function<void(RID &, char *)> func) {
    RM_ScanIterator scanner;
    if (scan(tableName, conditionAttribute, compOp, value, attributeNames, scanner) < 0)
        return -1;
    RID rid;
    char databuf[PAGE_SIZE];
    while ((scanner.getNextTuple(rid, databuf)) != RM_EOF) {
        func(rid, databuf);
    }
    scanner.close();
    return 0;
}

RC RelationManager::mapRIDs(const std::string &tableName, const std::string &conditionAttribute, const CompOp compOp,
                            const void *value,
                            const std::vector<std::string> &attributeNames, std::function<void(RID &, char *, FileHandle&)> func, FileHandle& fh) {
    RM_ScanIterator scanner;
    if (scan(tableName, conditionAttribute, compOp, value, attributeNames, scanner) < 0)
        return -1;
    RID rid;
    char databuf[PAGE_SIZE];
    while ((scanner.getNextTuple(rid, databuf)) != RM_EOF) {
        func(rid, databuf, fh);
    }
    scanner.close();
    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    attrs.clear();
    int table_id, version;
    if ((table_id = getTableIdVersion(tableName, &version)) < 0)
        return -1;

    return getAttributes(tableName, attrs, version);
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int version) {
    int table_id;
    if ((table_id = getTableIdVersion(tableName)) < 0)
        return -1;
    auto func = [&attrs, version](RID &rid, char *data){
        int pos, this_version;
        auto attrres = catalogColumnToAttribute(data, &pos, &this_version);
        if(this_version != version)
            return;
        if (pos > attrs.size()) attrs.resize(pos);
        attrs[pos - 1] = attrres;
    };

    return mapRIDs("Columns", "table-id", EQ_OP, &table_id, CatalogColumnTupleNames, func);
}



RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    //先insertTuple获得RID后，对于该表上的所有索引（scan+mapRIDs)，逐一打开index文件进行插入
    int table_id;
    std::vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) < 0) return -1;
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1; // hard code filename
    int version;
    if(table_id = getTableIdVersion(tableName, &version) <= MaxCatalogID)
        return -1;
    if( rbfm.insertRecord(file, recordDescriptor, data, rid, version) <0 ) return -1;

    char databuf[PAGE_SIZE];
    for(int i=0;i<recordDescriptor.size();i++) {
        std::string attributeName =  recordDescriptor[i].name;
        auto &ix = IndexManager::instance();
        std::string file_name = makeIndexFileName(tableName, attributeName);
        IXFileHandle ixfile;
        if(ix.openFile(file_name, ixfile) < 0) continue;
        if(testBit((const char*)data, i)) continue; // ignore null value
        rbfm.readAttribute(file,recordDescriptor,rid,attributeName,databuf);
        // skip null indicator
        int indicator_len = getIndicatorLen(recordDescriptor.size());
        ix.insertEntry(ixfile,recordDescriptor[i],(databuf + indicator_len),rid);
    }

    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    if(getTableIdVersion(tableName) <= MaxCatalogID)
        return -1;
    std::vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) < 0) return -1;
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1;
    
    char databuf[PAGE_SIZE];
    for(int i=0;i<recordDescriptor.size();i++) {
        std::string attributeName =  recordDescriptor[i].name;
        auto &ix = IndexManager::instance();
        std::string file_name = makeIndexFileName(tableName, attributeName);
        IXFileHandle ixfile;
        if(ix.openFile(file_name, ixfile) < 0) continue;
        rbfm.readAttribute(file,recordDescriptor,rid,attributeName,databuf);
        // skip null indicator
        int indicator_len = getIndicatorLen(recordDescriptor.size());
        ix.deleteEntry(ixfile,recordDescriptor[i],databuf+indicator_len,rid);
    }

    return rbfm.deleteRecord(file,recordDescriptor,rid);

}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    //update 默认用最新的attrs
    std::vector<Attribute> to_attrs;
    if(getAttributes(tableName, to_attrs) < 0) return -1;
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1;
    int version;
    int table_id = getTableIdVersion(tableName,&version);
    if(table_id <= MaxCatalogID) return -1;
    return rbfm.updateRecord(file,to_attrs,data,rid, version);
}


RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    /* std::vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) < 0) return -1;
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1; // hard code filename
    return rbfm.readRecord(file, recordDescriptor, rid, data); */
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1; // hard code filename
    char databuf[PAGE_SIZE];
    int from_version;
    SlotItem slot; // not beautiful
    if(rbfm.readRawRecord(file, rid, databuf, from_version, slot) < 0)
        return -1;

    std::vector<Attribute> from_attrs, to_attrs;
    if(getAttributes(tableName, from_attrs, from_version) < 0) return -1;
    if(getAttributes(tableName, to_attrs) < 0) return -1;
    rbfm.convertVersion(databuf, (char*)data, from_attrs, to_attrs, slot);
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return RecordBasedFileManager::instance().printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    FileHandle file;
    auto &rbfm = RecordBasedFileManager::instance();
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1; // hard code filename
    char databuf[PAGE_SIZE];
    int from_version;
    SlotItem slot; // not beautiful
    if(rbfm.readRawRecord(file, rid, databuf, from_version, slot) < 0)
        return -1;

    std::vector<Attribute> from_attrs, to_attrs;
    if(getAttributes(tableName, from_attrs, from_version) < 0) return -1;
    if(getAttributes(tableName, to_attrs) < 0) return -1;
    auto dict = getFieldIndex(from_attrs);
    auto to_dict = getFieldIndex(to_attrs);
    if(to_dict.count(attributeName) == 0) return -1;
    if (dict.count(attributeName) == 0){ // is null => only copy null indicator
        int indicator_len = getIndicatorLen(slot.field_num);
        memcpy(data, databuf + slot.data_size + sizeof(TypeSchemaVersion) + sizeof(TypeSlotNum) + slot.field_num * sizeof(TypeOffset), indicator_len);
        return 0;
    }
    return rbfm.readAttribute(file,from_attrs,rid,attributeName,data);
}

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    auto &rbfm = RecordBasedFileManager::instance();
    if (tableName == "Tables") {
        FileHandle file;
        rbfm.openFile(CATALOG_TABLE, file);
        RBFM_ScanIterator scanner;
        rbfm.scan(file, getCatalogTableAttribute(), conditionAttribute, compOp, value, attributeNames, scanner);
        rm_ScanIterator.setScanner(scanner);
        return 0;
    } else if (tableName == "Columns") {
        FileHandle file;
        rbfm.openFile(CATALOG_COLUMN, file);
        RBFM_ScanIterator scanner;
        rbfm.scan(file, getCatalogColumnAttribute(), conditionAttribute, compOp, value, attributeNames, scanner);
        rm_ScanIterator.setScanner(scanner);
        return 0;
    }
    // other tables
    std::vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) < 0) {
        // std::cerr << "[scan] getAttributes fail" << std::endl;
        return -1;
    }
    RBFM_ScanIterator scanner;
    FileHandle file;
    if (rbfm.openFile(tableName + postfix, file) < 0) return -1; // hard code filename
    if (rbfm.scan(file, recordDescriptor, conditionAttribute, compOp, value, attributeNames, scanner) < 0) return -1;
    rm_ScanIterator.setScanner(scanner);
    return 0;
}

RC RelationManager::indexScan(const std::string &tableName, const std::string &attributeName, const void *lowKey, const void *highKey,
                 bool lowKeyInclusive, bool highKeyInclusive, RM_IndexScanIterator &rm_IndexScanIterator) {
    IXFileHandle ixfile;
    auto& ix = IndexManager::instance();
    if(ix.openFile(makeIndexFileName(tableName, attributeName), ixfile) < 0){
        return -1;
    }
    // attribute
    Attribute attr;
    int table_id = getTableIdVersion(tableName);
    if(table_id < 0) return -1;
    if(getAttribute(table_id, attributeName, attr) < 0) return -1;
    IX_ScanIterator scanner;
    if(ix.scan(ixfile, attr, lowKey, highKey, lowKeyInclusive, highKey, scanner) < 0)
        return -1;
    
    rm_IndexScanIterator.setScanner(scanner);
    return 0;
}

RC RelationManager::getAttribute(int table_id, const std::string& attrName, Attribute& attr, int* posi){
    bool hit = false;
    RM_ScanIterator rm_scanner;
    if (scan("Columns", "table-id", EQ_OP, &table_id, CatalogColumnTupleNames, rm_scanner) < 0)
        return -1;
    RID rid;
    char databuf[240];
    while ((rm_scanner.getNextTuple(rid, databuf)) != RM_EOF) {
        attr = catalogColumnToAttribute(databuf, posi);
        if(attr.name == attrName){
            hit = true;
            break;
        }
    }
    rm_scanner.close();
    if(!hit) return -1;
    return 0;
}
// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    int version;//need to complete, latest version of tableName
    RID rid;
    int table_id = getTableIdVersion(tableName, &version, &rid);//?
    //version ++;
    
    auto &rbfm = RecordBasedFileManager::instance();
    FileHandle file;
    //modify catlog table
    if (rbfm.openFile(CATALOG_TABLE, file) < 0) {
        return -1;
    }

    std::vector<char> databuf;
    auto recordDescriptor = getCatalogTableAttribute();
    prepareCatalogTableData(databuf, recordDescriptor, table_id, tableName, tableName + postfix, version+1);
    rbfm.updateRecord(file, recordDescriptor, databuf.data(), rid);

    if (rbfm.openFile(CATALOG_COLUMN, file) < 0) {
        return -1;
    }
    recordDescriptor = getCatalogColumnAttribute();
    std::vector<Attribute> attrs; // need to complete, latest Attributes of tableName without attributeName
    getAttributes(tableName,attrs,version);
    for (auto it = attrs.begin(); it != attrs.end(); ++it) {
        if (it->name == attributeName){
            attrs.erase(it);
            break;
        }
    }
    for (int i = 0; i < attrs.size(); ++i) {
        prepareCatalogColumnData(databuf, recordDescriptor, table_id, attrs[i], i + 1, version+1);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    
    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    int version;//need to complete, latest version of tableName
    RID rid;
    int table_id = getTableIdVersion(tableName, &version, &rid);//?
    //version ++;
    
    auto &rbfm = RecordBasedFileManager::instance();
    FileHandle file;
    //modify catlog table
    if (rbfm.openFile(CATALOG_TABLE, file) < 0) {
        return -1;
    }

    std::vector<char> databuf;
    auto recordDescriptor = getCatalogTableAttribute();
    prepareCatalogTableData(databuf, recordDescriptor, table_id, tableName, tableName + postfix, version+1);
    rbfm.updateRecord(file, recordDescriptor, databuf.data(), rid);


    if (rbfm.openFile(CATALOG_COLUMN, file) < 0) {
        return -1;
    }
    recordDescriptor = getCatalogColumnAttribute();
    std::vector<Attribute> attrs; // need to complete, latest Attributes of tableName
    getAttributes(tableName,attrs,version);
    attrs.push_back(attr);
    for (int i = 0; i < attrs.size(); ++i) {
        prepareCatalogColumnData(databuf, recordDescriptor, table_id, attrs[i], i + 1, version+1);
        rbfm.insertRecord(file, recordDescriptor, databuf.data(), rid);
    }
    
    return 0;
}
