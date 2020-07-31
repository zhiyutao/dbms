#include "ix.h"
#include <unistd.h>
#include <vector>
#include <memory.h>
#include <float.h>
/* ========== compare functions ============== */
int compareRID(const RID& a, const RID& b){
    if(a.pageNum < b.pageNum) return -1;
    else if(a.pageNum > b.pageNum) return 1;
    else if(a.slotNum < b.slotNum) return -1;
    else if(a.slotNum > b.slotNum) return 1;
    return 0;
}
template<typename T>
int compareNumIndexItem(const IndexItem& a, const IndexItem& b){
    T v1 = *(T*)a.value.data(), v2 = *(T*)b.value.data();
    if(v1 == v2) return 0;
    else if(v1 < v2) return -1;
    else return 1;
}
template<typename T>
int compareNumIndexItemWithRID(const IndexItem& a, const IndexItem& b){
    T v1 = *(T*)a.value.data(), v2 = *(T*)b.value.data();
    if(v1 == v2){
        return compareRID(a.rid, b.rid);
    }
    else if(v1 < v2) return -1;
    else return 1;
}
int compareVarcharIndexItem(const IndexItem& a, const IndexItem& b){
    if(a.value < b.value) return -1;
    else if(a.value == b.value) return 0;
    else return 1;
}
int compareVarcharIndexItemWithRID(const IndexItem& a, const IndexItem& b){
    if(a.value < b.value) return -1;
    else if(a.value == b.value) return compareRID(a.rid, b.rid);
    else return 1;
}
/* ========== functions ============== */
IndexItem makeCompositeIndex(const Attribute& attr, const void* key, const RID& rid){
    IndexItem res;
    switch(attr.type) {
        case TypeInt:
        case TypeReal:
            pushBackTo(res.value, (const char*)key, 4); break;
        case TypeVarChar:
            pushBackTo(res.value, (const char*)key + 4, *(int*)key); break;
    }
    res.rid = rid;
    return res;
}
int binarySearchUpperBound(char* page, const IndexItem& indexValue, CompFunc comp) {
    // return the index of the first item > indexValue
    int a = 0, b = IndexPage::getSlotCount(page) - 2, mid; // omit the final child-only record
    if(b < 0) return 0;
    while(a < b){
        mid = a + (b-a) / 2;
        if(comp(indexValue,IndexPage::readIndexItem(page, mid)) < 0){
            b = mid;
        } else{
            a = mid + 1;
        }
    }
    if(comp(indexValue, IndexPage::readIndexItem(page, a)) >= 0)
        return a + 1;
    return a;
}
int binarySearchLowerBound(char* page,  const IndexItem& indexValue, CompFunc comp) {
    // return the index of the first item >= indexValue
    int a = 0, b = IndexPage::getSlotCount(page) - 2, mid; // omit the final child-only record
    if(b < 0) return 0; 
    while(a < b){
        mid = a + (b-a) / 2;
        if(comp(indexValue,IndexPage::readIndexItem(page, mid)) > 0){
            a = mid + 1;
        } else{
            b = mid;
        }
    }
    if(comp(indexValue, IndexPage::readIndexItem(page, a)) > 0)
        return a + 1;
    return a;
}

bool recursiveInsert(IXFileHandle& ixFileHandle, int parPageNum, int currPageNum, IndexItem& indexitem, 
    int& leftAddPageNum, int& rightAddPageNum, std::vector<char>& upflowIndexValue, CompFunc comp, bool& valid_insertion){
    /* return true: if page overflow */
    char pagebuf[PAGE_SIZE];
    if(ixFileHandle.readPage(currPageNum, pagebuf) < 0){
        std::cerr << "[insertError] " << parPageNum<<" "<<currPageNum << " " << ixFileHandle.getNumberOfPages() << std::endl;
        valid_insertion = false;
        return false;
    }
    
    if(IndexPage::getNextLeafId(pagebuf) < -1){ // is not leaf
        int i = binarySearchUpperBound(pagebuf, indexitem, comp);
        SlotItem& slotref = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        int leftChildPageNum = *(int*)(pagebuf+slotref.offset);
        if(recursiveInsert(ixFileHandle, currPageNum, leftChildPageNum, indexitem, leftAddPageNum, rightAddPageNum, upflowIndexValue, comp, valid_insertion)){ // child page overflow
            // if(leftAddPageNum == -1 || rightAddPageNum == -1) std::cerr << "[InsertError] leftPage and rightPage"<<std::endl;
            // sizeof( composite key + slot + childPageNum)
            if(upflowIndexValue.size() + sizeof(SlotItem) + sizeof(int) <= IndexPage::getEmptySize(pagebuf)){ // has enough space
                IndexPage::insertValueTo(pagebuf, leftAddPageNum, upflowIndexValue, i);
                IndexPage::setChildPageNum(pagebuf, rightAddPageNum, i + 1);
                ixFileHandle.writePage(currPageNum, pagebuf);
                return false;
            } else {
                char newpage[PAGE_SIZE];
                IndexPage::setChildPageNum(pagebuf, rightAddPageNum, i); // must be done at first !!!
                IndexPage::insertValueAndSplitPage(pagebuf, newpage, leftAddPageNum, upflowIndexValue, i);
                IndexPage::setNextLeafId(newpage, NONLEAF); // non leaf node !!!
                leftAddPageNum = currPageNum;
                rightAddPageNum = ixFileHandle.getNumberOfPages();
                // non leaf split, uplift left page
                int oldSlotn = IndexPage::getSlotCount(pagebuf);
                upflowIndexValue = IndexPage::readRawIndex(pagebuf, oldSlotn-2); // the second last value
                SlotItem *secondlast = (SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + (oldSlotn-2) * sizeof(SlotItem));
                int child = *(int*)(pagebuf + secondlast->offset);
                secondlast->offset += secondlast->data_size - sizeof(child);
                memcpy(pagebuf + secondlast->offset, &child, sizeof(child));
                IndexPage::setStackTop(pagebuf, secondlast->offset);
                secondlast->data_size = sizeof(child); secondlast->field_num = 1;
                IndexPage::setSlotTableLen(pagebuf, (oldSlotn-1) * sizeof(SlotItem));
                // flush to disk
                ixFileHandle.appendPage(newpage);
                ixFileHandle.writePage(currPageNum, pagebuf);
                return true;
            }
        }
    } else { // leaf
        IndexPage::garbageSlotCollection(pagebuf);
        int i = binarySearchLowerBound(pagebuf, indexitem, comp);
        SlotItem& slotref = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        // reinsert a same index
        if(slotref.metadata_size != -1 && i < IndexPage::getSlotCount(pagebuf) - 1 && comp(indexitem, IndexPage::readIndexItem(pagebuf, i)) == 0){
            valid_insertion = false; return false;
        }
        std::vector<char> indexValue = indexitem.value; 
        pushBackTo(indexValue, (const char*)&(indexitem.rid), sizeof(RID));
        // sizeof( composite key + slot + childPageNum)
        if(indexValue.size() + sizeof(SlotItem) + sizeof(int) <= IndexPage::getEmptySize(pagebuf)){ // has enough space
            IndexPage::insertValueTo(pagebuf, -1, indexValue, i);
            ixFileHandle.writePage(currPageNum, pagebuf);
            return false;
        } else {
            char newpage[PAGE_SIZE];
            int leafid = IndexPage::getNextLeafId(pagebuf);
            IndexPage::insertValueAndSplitPage(pagebuf, newpage, -1, indexValue, i);
            leftAddPageNum = currPageNum;
            rightAddPageNum = ixFileHandle.getNumberOfPages();
            upflowIndexValue = IndexPage::readRawIndex(newpage, 0);
            // link leaf node
            IndexPage::setNextLeafId(newpage, leafid);
            IndexPage::setNextLeafId(pagebuf, rightAddPageNum);
            // flush to disk
            ixFileHandle.writePage(currPageNum, pagebuf);
            ixFileHandle.appendPage(newpage);
            return true;
        }
    }
    return false;
}

RC recursiveDelete (IXFileHandle& ixFileHandle,int currPageNum, IndexItem& indexitem, CompFunc comp){
    char pagebuf[PAGE_SIZE];
    if(ixFileHandle.readPage(currPageNum, pagebuf) < 0) return -1;

    while(IndexPage::getNextLeafId(pagebuf) < -1) {
        // is not leaf
        int i = binarySearchUpperBound(pagebuf, indexitem, comp);
        SlotItem &slotref = *(SlotItem *) (pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        currPageNum = *(int *) (pagebuf + slotref.offset);
        if(ixFileHandle.readPage(currPageNum, pagebuf) < 0) return -1;
    }
    //is leaf
    int i = binarySearchLowerBound(pagebuf, indexitem, comp);
    SlotItem& dSlotref = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    if(i == IndexPage::getSlotCount(pagebuf) - 1 || dSlotref.metadata_size == -1 || comp(indexitem, IndexPage::readIndexItem(pagebuf, i)) != 0) {
        // want to delete a nonexsit key
        return -1;
    }
    // IndexPage::deleteIndexFrom(pagebuf, i);
    dSlotref.metadata_size = -1;
    IndexPage::setEmptySlotFlag(pagebuf, true);
    ixFileHandle.writePage(currPageNum, pagebuf);
    return 0;

    //        if(recursiveDelete(ixFileHandle, currPageNum, leftChildPageNum, indexitem, mergeDirection, i, comp, valid_deletion)) {
    //
    //
    //
    //            //子节点存在merge
    //            /* if (mergeDirection<0){
    //                //子节点与左兄弟merge
    //                SlotItem& tmpslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + (i-1) * sizeof(SlotItem));
    //                if(sizeof(SlotItem) + tmpslot.data_size + IndexPage::getEmptySize(pagebuf) <= PAGE_SIZE/2 ){
    //                    //size >= 50%
    //                    // move index part
    //                    SlotItem& startslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + (i-1) * sizeof(SlotItem));
    //                    SlotItem& endslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE +  IndexPage::getSlotTableLen(pagebuf) - sizeof(SlotItem));
    //                    TypeOffset movestart = endslot.offset;
    //                    TypeOffset moveend = startslot.offset;
    //                    int len = startslot.data_size;
    //                    IndexPage::compress(pagebuf, movestart, moveend, len);
    //                    // move slot table part
    //                    movestart = IndexPage::PAGEHEADSIZE + (i+1) * sizeof(SlotItem);
    //                    moveend = IndexPage::PAGEHEADSIZE + IndexPage::getSlotTableLen(pagebuf);
    //                    IndexPage::compress(pagebuf, movestart, moveend, -sizeof(SlotItem));
    //                    ixFileHandle.writePage(currPageNum, pagebuf);
    //                    return false;
    //                }
    //                else {
    //                    //size<50%
    //                    //借
    //                    char tmpPage[PAGE_SIZE];
    //
    //                    if(parIndex>0) {
    //                        //有左兄弟
    //                        if (ixFileHandle.readPage(parPageNum, tmpPage) < 0) return -1;
    //                        SlotItem& tmpslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE + (parIndex-1) * sizeof(SlotItem));
    //                        int leftPageNum = *(int *) (tmpPage + tmpslot.offset);
    //                        if (ixFileHandle.readPage(leftPageNum, tmpPage) < 0) return -1;
    //                        SlotItem& moveslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE+IndexPage::getSlotTableLen(tmpPage)-sizeof(SlotItem));
    //                        if (IndexPage::getEmptySize(tmpPage)+sizeof(SlotItem)+moveslot.data_size <= PAGE_SIZE/2) {
    //                            //可从左借 借最后个
    //                            int slotIndex = IndexPage::getSlotCount(tmpPage)-1;
    //                            std::vector<char> moveindex = IndexPage::readRawIndex(tmpPage,slotIndex);
    //                            int childPageNum = *(int*)(tmpPage+moveslot.data_size);
    //                            ixFileHandle.writePage(leftPageNum,tmpPage);
    //                            IndexPage::insertValueTo(pagebuf,childPageNum,moveindex,IndexPage::getSlotCount(pagebuf));
    //                            ixFileHandle.writePage(currPageNum,pagebuf);
    //                            return false;
    //                        }
    //                    }
    //
    //                    if (ixFileHandle.readPage(parPageNum, tmpPage) < 0) return -1;
    //                    if(IndexPage::getSlotCount(tmpPage)>parIndex+1){
    //                        //有右兄弟
    //                        SlotItem& tmpslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE + (parIndex+1) * sizeof(SlotItem));
    //                        int rightPageNum = *(int *) (tmpPage + tmpslot.offset);
    //                        if (ixFileHandle.readPage(rightPageNum, tmpPage) < 0) return -1;
    //                        SlotItem &moveslot = *(SlotItem *) (tmpPage + IndexPage::PAGEHEADSIZE);
    //                        if (IndexPage::getEmptySize(tmpPage) + sizeof(SlotItem) + moveslot.data_size <= PAGE_SIZE / 2) {
    //                            //可从右借 借第一个
    //                            int slotIndex = 0;
    //                            std::vector<char> moveindex = IndexPage::readRawIndex(tmpPage, slotIndex);
    //                            int childPageNum = *(int *) (tmpPage + moveslot.data_size);
    //                            ixFileHandle.writePage(rightPageNum, tmpPage);
    //                            IndexPage::insertValueTo(pagebuf, childPageNum, moveindex, IndexPage::getSlotCount(pagebuf));
    //                            ixFileHandle.writePage(currPageNum, pagebuf);
    //                            return false;
    //                        }
    //                    }
    //
    //                    //merge
    //                    if (ixFileHandle.readPage(parPageNum, tmpPage) < 0) return -1;
    //                    if(IndexPage::getSlotCount(tmpPage)>parIndex+1){
    //                        //有右兄弟
    //                        SlotItem& tmpslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE + (parIndex+1) * sizeof(SlotItem));
    //                        int rightPageNum = *(int *) (tmpPage + tmpslot.offset);
    //                        if (ixFileHandle.readPage(rightPageNum, tmpPage) < 0) return -1;
    //                        if (IndexPage::getEmptySize(tmpPage)>=PAGE_SIZE-IndexPage::getEmptySize(pagebuf)-IndexPage::PAGEHEADSIZE) {
    //                            //右兄弟够merge
    //                            for(int j=0;j<IndexPage::getSlotCount(pagebuf);j++) {
    //                                SlotItem& sloti = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    //                                std::vector<char> moveindex ;
    //                                int len = slotref.data_size - sizeof(int);
    //                                pushBackTo(moveindex, pagebuf + slotref.offset + sizeof(int), len);
    //                                int childpage = *(int *)(pagebuf + slotref.offset);
    //                                //待完善 我猜有函数能在中间节点插入
    //                                //IndexPage::insertValueTo(tmpPage,childpage,moveindex,IndexPage::getSlotCount(tmpPage));
    //                            }
    //                            ixFileHandle.writePage(rightPageNum,tmpPage);
    //                            //待完善 改链表和元数据页空页头结点，设置mergedChildPageNum 返回
    //                        }
    //
    //                    }
    //
    //                    if(parIndex>0) {
    //                        //有左兄弟
    //                        if (ixFileHandle.readPage(parPageNum, tmpPage) < 0) return -1;
    //                        SlotItem& tmpslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE + (parIndex-1) * sizeof(SlotItem));
    //                        int leftPageNum = *(int *) (tmpPage + tmpslot.offset);
    //                        if (ixFileHandle.readPage(leftPageNum, tmpPage) < 0) return -1;
    //                        if (IndexPage::getEmptySize(tmpPage)>=PAGE_SIZE-IndexPage::getEmptySize(pagebuf)-IndexPage::PAGEHEADSIZE) {
    //                            //左兄弟够merge
    //                            for(int j=0;j<IndexPage::getSlotCount(pagebuf);j++) {
    //                                SlotItem& sloti = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    //                                std::vector<char> moveindex ;
    //                                int len = slotref.data_size - sizeof(int);
    //                                pushBackTo(moveindex, pagebuf + slotref.offset + sizeof(int), len);
    //                                int childpage = *(int *)(pagebuf + slotref.offset);
    //                                //待完善 我猜有函数能在中间节点插入
    //                                //IndexPage::insertValueTo(tmpPage,childpage,moveindex,IndexPage::getSlotCount(tmpPage));
    //                            }
    //                            ixFileHandle.writePage(leftPageNum,tmpPage);
    //                            //待完善 改链表和元数据页空页头结点，设置mergedChildPageNum 返回
    //                        }
    //                    }
    //
    //
    //                    //都不能merge, 直接删
    //                    // move index part
    //                    SlotItem& startslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + (i-1) * sizeof(SlotItem));
    //                    SlotItem& endslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE +  IndexPage::getSlotTableLen(pagebuf) - sizeof(SlotItem));
    //                    TypeOffset movestart = endslot.offset;
    //                    TypeOffset moveend = startslot.offset;
    //                    int len = startslot.data_size;
    //                    IndexPage::compress(pagebuf, movestart, moveend, len);
    //                    // move slot table part
    //                    movestart = IndexPage::PAGEHEADSIZE + (i+1) * sizeof(SlotItem);
    //                    moveend = IndexPage::PAGEHEADSIZE + IndexPage::getSlotTableLen(pagebuf);
    //                    IndexPage::compress(pagebuf, movestart, moveend, -sizeof(SlotItem));
    //                    ixFileHandle.writePage(currPageNum, pagebuf);
    //                    return false;
    //                }
    //            } */
    //        }
    //    } else {// leaf
    //        int i = binarySearchLowerBound(pagebuf, indexitem, comp);
    //        if(i == IndexPage::getSlotCount(pagebuf) - 1 || comp(indexitem, IndexPage::readIndexItem(pagebuf, i)) != 0) { // want to delete a nonexsit key
    //            valid_deletion = false;
    //            return false;
    //        }
    //        if((indexitem.value.size() + sizeof(RID) + sizeof(SlotItem) + sizeof(int) + IndexPage::getEmptySize(pagebuf)) * 2 <= PAGE_SIZE ){
    //            //size >= 50%
    //            // move index part
    //            IndexPage::deleteIndexFrom(pagebuf, i);
    //            ixFileHandle.writePage(currPageNum, pagebuf);
    //            return false;
    //        } else {
    //            //size < 50%
    //            /*
    //            char tmpPage[PAGE_SIZE];
    //            //借
    //            int siblingId;
    //            if((siblingId = IndexPage::getNextLeafId(pagebuf)) != -1) {
    //                //有右兄弟
    //                if (ixFileHandle.readPage(siblingId, tmpPage) < 0) return -1;
    //                SlotItem &moveslot = *(SlotItem *) (tmpPage + IndexPage::PAGEHEADSIZE);
    //                if (IndexPage::getEmptySize(tmpPage) + sizeof(SlotItem) + moveslot.data_size <= PAGE_SIZE / 2) {
    //                    //可从右借 借第一个
    //                    int slotIndex = 0;
    //                    std::vector<char> moveindex = IndexPage::readRawIndex(tmpPage, slotIndex);
    //                    int childPageNum = *(int *) (tmpPage + moveslot.data_size);
    //                    ixFileHandle.writePage(currPageNum + 1, tmpPage);
    //                    IndexPage::insertValueTo(pagebuf, childPageNum, moveindex, IndexPage::getSlotCount(pagebuf));
    //                    ixFileHandle.writePage(currPageNum, pagebuf);
    //                    return false;
    //                }
    //            }
    //
    //            if (ixFileHandle.readPage(currPageNum-1, tmpPage) == 1 && IndexPage::getNextLeafId(tmpPage)>=-1) {
    //                //有左兄弟
    //                SlotItem& moveslot = *(SlotItem*)(tmpPage + IndexPage::PAGEHEADSIZE+IndexPage::getSlotTableLen(tmpPage)-
    //                                                  sizeof(SlotItem));
    //                if (IndexPage::getEmptySize(tmpPage)+sizeof(SlotItem)+moveslot.data_size <= PAGE_SIZE/2) {
    //                    //可从左借 借最后个
    //                    int slotIndex = IndexPage::getSlotCount(tmpPage)-1;
    //                    std::vector<char> moveindex = IndexPage::readRawIndex(tmpPage,slotIndex);
    //                    int childPageNum = *(int*)(tmpPage+moveslot.data_size);
    //                    ixFileHandle.writePage(currPageNum-1,tmpPage);
    //                    IndexPage::insertValueTo(pagebuf,childPageNum,moveindex,IndexPage::getSlotCount(pagebuf));
    //                    ixFileHandle.writePage(currPageNum,pagebuf);
    //                    return false;
    //                }
    //            }
    //
    //            //merge
    //            if(ixFileHandle.readPage(currPageNum+1, tmpPage) == 1 && IndexPage::getEmptySize(tmpPage)>=PAGE_SIZE-IndexPage::getEmptySize(pagebuf)-IndexPage::PAGEHEADSIZE) {
    //                //右兄弟够merge
    //                for(int j=0;j<IndexPage::getSlotCount(pagebuf);j++) {
    //                    std::vector<char> moveindex = IndexPage::readRawIndex(pagebuf,j);
    //                    IndexPage::insertValueTo(tmpPage,-1,moveindex,IndexPage::getSlotCount(tmpPage));
    //                }
    //                ixFileHandle.writePage(currPageNum+1,tmpPage);
    //                //待完善 改链表和元数据页空页头结点，设置mergedChildPageNum 返回
    //            }
    //
    //            if(ixFileHandle.readPage(currPageNum-1, tmpPage) == 1 && IndexPage::getEmptySize(tmpPage)>=PAGE_SIZE-IndexPage::getEmptySize(pagebuf)-IndexPage::PAGEHEADSIZE) {
    //                //左兄弟够merge
    //                for(int j=0;j<IndexPage::getSlotCount(pagebuf);j++) {
    //                    std::vector<char> moveindex = IndexPage::readRawIndex(pagebuf,j);
    //                    IndexPage::insertValueTo(tmpPage,-1,moveindex,IndexPage::getSlotCount(tmpPage));
    //                }
    //                ixFileHandle.writePage(currPageNum-1,tmpPage);
    //                //待完善 改链表和元数据页空页头结点，设置mergedChildPageNum 返回
    //            } */
    //
    //            //都不能merge, 直接删
    //            // move index part
    //            IndexPage::deleteIndexFrom(pagebuf, i);
    //            ixFileHandle.writePage(currPageNum, pagebuf);
    //            return false;
    //        }
    //    }
    //    return false;
}

void printPage(IXFileHandle &ixFileHandle, int currPageNum, std::function<std::string(char*, const SlotItem&)> p) {
    char pagebuf[PAGE_SIZE];
    if(ixFileHandle.readPage(currPageNum, pagebuf) < 0) return;
    int nextLeaf = IndexPage::getNextLeafId(pagebuf);

    if(nextLeaf < -1) {//non-leaf node
        int slotCount = IndexPage::getSlotCount(pagebuf);
        std::cout<<"{\"keys\":[";
        for(int i=0;i <slotCount - 1;i++ ) { // don't print the child-only one
            SlotItem &slot = *(SlotItem *) (pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
            if(slot.metadata_size==-1) continue;
            char *key_start = pagebuf + slot.offset + sizeof(int);
            std::cout << "\""<<p(key_start, slot)<<"\"";
            if(i!=slotCount - 2) std::cout <<",";
        }
        std::cout << "],"<<std::endl;
        std::cout<<"\"children\": ["<<std::endl;
        for(int i=0;i <slotCount;i++ ) {
            SlotItem &slot = *(SlotItem *) (pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
            if(slot.metadata_size==-1) continue;
            int childPageNum = *(int*)( pagebuf + slot.offset);
            printPage(ixFileHandle,childPageNum,p);
            if(i!=slotCount-1) std::cout <<","<<std::endl;
        }
        std::cout<<"]}";
    } else {//leaf node
        int slotCount = IndexPage::getSlotCount(pagebuf) - 1; // don't print the child-only one
        std::cout<<"{\"keys\":[";
        std::string s = "";
        for(int i=0;i <slotCount;i++ ) {
            SlotItem &slot = *(SlotItem *) (pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
            if(slot.metadata_size==-1) continue;
            char *key_start = pagebuf + slot.offset + sizeof(int);
            std::string key = p(key_start, slot);
            if(key != s) {
                if(s!="") std::cout<<"]\",";
                std::cout<<("\""+key+":[");
                char *RID_start = pagebuf + slot.offset+slot.data_size- sizeof(RID);
                std::cout<<"("<<*(unsigned*)RID_start<<","<<*(unsigned*)(RID_start+ sizeof(unsigned)) <<")";
                s=key;
            } else {
                char *RID_start = pagebuf + slot.offset+slot.data_size- sizeof(RID);
                std::cout<<",("<<*(unsigned*)RID_start<<","<<*(unsigned*)(RID_start+ sizeof(unsigned))<<")";
            }

            if(i == slotCount - 1) std::cout <<"]\"";
        }
        std::cout << "]}"<<std::endl;
    }
}

std::string printInt(char* start, const SlotItem& s){
    return std::to_string(*(int*)(start));
}
std::string printFloat(char* start, const SlotItem& s){
    return std::to_string(*(float*)(start));
}
std::string printVarChar(char* start, const SlotItem& slot){
    std::string s = "";
    int len = slot.data_size - sizeof(RID) - sizeof(unsigned);
    for(int j=0;j<len;j++) {
        s += start[j];
    }
    return s;
}

/* ========== IndexManager ========== */
IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    return wCreateFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return remove(fileName.c_str());
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    FILE *fPointer = NULL;
    fPointer = fopen(fileName.c_str(), "r+");
    if (fPointer == NULL) {
        return -1;
    } else if(ixFileHandle.isOpen()) { 
        return -1;
    } else {
        ixFileHandle.setFile(fPointer);
        return 0;
    }
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return ixFileHandle.releaseFile();
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(!ixFileHandle.isOpen()) return -1;
    int currPageNum = ixFileHandle.getRootPageNum(), parPageNum = -1;
    char pagebuf[PAGE_SIZE];
    if(currPageNum == -1){
        IndexPage::InitializePage(pagebuf);
        IndexPage::appendTailChildPointer(pagebuf);
        ixFileHandle.appendPage(pagebuf);
        currPageNum = ixFileHandle.getNumberOfPages() - 1;
        ixFileHandle.setRootPageNum(currPageNum);
    }

    int leftAddPageNum = -1, rightAddPageNum = -1;
    std::vector<char> upflowIndexValue, indexValue;
    IndexItem indexitem = makeCompositeIndex(attribute, key, rid);
    CompFunc comp;
    switch(attribute.type) {
        case TypeInt: {comp = compareNumIndexItemWithRID<int>;break;}
        case TypeReal: {comp = compareNumIndexItemWithRID<float>;break;}
        case TypeVarChar:{comp = compareVarcharIndexItemWithRID;break;}
    }
    bool valid_insertion = true;
    if(recursiveInsert(ixFileHandle, parPageNum, currPageNum, indexitem, leftAddPageNum, rightAddPageNum, upflowIndexValue, comp, valid_insertion)){
        // new root
        IndexPage::InitializePage(pagebuf);
        IndexPage::appendTailChildPointer(pagebuf);
        IndexPage::setNextLeafId(pagebuf, NONLEAF); // non leaf
        IndexPage::setChildPageNum(pagebuf, rightAddPageNum, 0);
        IndexPage::insertValueTo(pagebuf, leftAddPageNum, upflowIndexValue, 0);
        ixFileHandle.appendPage(pagebuf);
        // update root page number
        ixFileHandle.setRootPageNum(ixFileHandle.getNumberOfPages() - 1);
    }
    if(!valid_insertion) return -1;
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    if(!ixFileHandle.isOpen()) return -1;
    int currPageNum = ixFileHandle.getRootPageNum(), parPageNum = -1;
    if(currPageNum == -1) return -1;

    IndexItem indexitem = makeCompositeIndex(attribute, key, rid);
    CompFunc comp;
    switch(attribute.type) {
        case TypeInt: {comp = compareNumIndexItemWithRID<int>;break;}
        case TypeReal: {comp = compareNumIndexItemWithRID<float>;break;}
        case TypeVarChar:{comp = compareVarcharIndexItemWithRID;break;}
    }
    return recursiveDelete(ixFileHandle,currPageNum, indexitem, comp);
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    if(!ixFileHandle.isOpen()) return -1;
    // binary search until leaf
    int currPageNum = ixFileHandle.getRootPageNum();
    if(currPageNum == -1) return -1;
    ix_ScanIterator.setParams(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    if(!ixFileHandle.isOpen()) return;
    int currPageNum = ixFileHandle.getRootPageNum();
    if (currPageNum == -1) return;
    AttrType type = attribute.type;
    std::function<std::string(char*, const SlotItem&)> p;
    switch(attribute.type) {
        case TypeInt: {p = printInt;break;}
        case TypeReal: {p = printFloat;break;}
        case TypeVarChar:{p = printVarChar;break;}
    }
    printPage(ixFileHandle,currPageNum,p);
}

/* ================= IX_ScanIterator ============== */
IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {}

void IX_ScanIterator::setParams(IXFileHandle& ixFileHandle, const Attribute& attribute, const void* lowKey, const void* highKey, bool lowKeyInclusive, bool highKeyInclusive) {
     inited_ = false;
     fh_ = ixFileHandle;
     attr_ = attribute;
     lowKeyInclusive_ = lowKeyInclusive;
     highKeyInclusive_ = highKeyInclusive;
     lowKeyNull_ = lowKey == NULL;
     highKeyNull_ = highKey == NULL;
     // serialize
     RID dummy;
     if(!lowKeyNull_) lowKey_ = makeCompositeIndex(attribute, lowKey, dummy);
     if(!highKeyNull_) highKey_ = makeCompositeIndex(attribute, highKey, dummy);
     switch(attr_.type) {
        case TypeVarChar: comp_ = compareVarcharIndexItem; break;
        case TypeInt: comp_ = compareNumIndexItem<int>;break;
        case TypeReal: comp_ = compareNumIndexItem<float>;break;
     }
 }
RC IX_ScanIterator::init() {
     // search the start leaf PageNum and SlotNum
     inited_ = true;
    int pagenum = fh_.getRootPageNum();
    if(fh_.readPage(pagenum, pagebuf_) < 0) return -1;

    int i;
    while(IndexPage::getNextLeafId(pagebuf_) < -1) {  // pagebuf_ is not a leaf node
        if(lowKeyNull_) i = 0;
        else i = binarySearchLowerBound(pagebuf_, lowKey_, comp_);

        SlotItem& slotref = *(SlotItem*)(pagebuf_ + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        pagenum = *(int*)(pagebuf_+slotref.offset);
        fh_.readPage(pagenum, pagebuf_);
    }
    currPageNum_ = pagenum;
    loadedPageNum_ = currSlotNum_;
    if(lowKeyNull_) {
        currSlotNum_ = 0;
    } else if(lowKeyInclusive_){
        currSlotNum_ = binarySearchLowerBound(pagebuf_, lowKey_, comp_);
    } else {
        currSlotNum_ = binarySearchUpperBound(pagebuf_, lowKey_, comp_);
    }
    return 0;
 }

ScanCODE IX_ScanIterator::_getNextEntry(RID& rid, void* key, int& nextLeafId) {
    if(currPageNum_ == -1) return ScanCODE::OVERPAGE;
    if(loadedPageNum_ != currPageNum_ ){
        if(fh_.readPage(currPageNum_, pagebuf_) < 0) return ScanCODE::OVERPAGE;
        loadedPageNum_ = currPageNum_;
    }
    if(currSlotNum_ >= IndexPage::getSlotCount(pagebuf_)-1){ // omit the child-only slot
        nextLeafId = IndexPage::getNextLeafId(pagebuf_);
        return ScanCODE::OVERSLOT;
    } 
    SlotItem& slotref = *(SlotItem*)(pagebuf_ + IndexPage::PAGEHEADSIZE + currSlotNum_ * sizeof(SlotItem));
    if(slotref.metadata_size == -1)
        return ScanCODE::INVALID_RECORD;

    // read index on (currPageNum_, currSlotNum_)
    auto item = IndexPage::readIndexItem(pagebuf_, currSlotNum_);
    // compare with highKey
    bool compres;
    if(highKeyNull_) compres = true;
    else if(highKeyInclusive_) compres = comp_(item, highKey_) <= 0;
    else compres = comp_(item,highKey_) < 0;
    
    if(!compres) return ScanCODE::OVERPAGE;
    rid = item.rid;
    switch(attr_.type){
        case TypeVarChar:
        {
            int varlen = item.value.size();
            memcpy(key, &varlen, sizeof(int));
            memcpy((char*)key + sizeof(int), item.value.data(), varlen);
            break;
        }
        default:
            memcpy(key, item.value.data(), item.value.size());
    }
    return ScanCODE::SUCC;
}
RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    if(!inited_){
        if(this->init() < 0) return IX_EOF;
    }
    ScanCODE errcode;
    int nextLeafId;
    while((errcode = _getNextEntry(rid, key, nextLeafId)) != ScanCODE::OVERPAGE){
        if(errcode == ScanCODE::SUCC){
            currSlotNum_ ++;
            return 0;
        } else if(errcode == ScanCODE::OVERSLOT){
            currSlotNum_ = 0;
            currPageNum_ = nextLeafId;
        } else if(errcode == ScanCODE::INVALID_RECORD){
            currSlotNum_ ++;
        } else {
            std::cerr << "Invalid ScanCODE" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    return IX_EOF;
}

RC IX_ScanIterator::close() {return 0;}
/* ================= IXFileHandle ================ */
IXFileHandle::IXFileHandle() {}

IXFileHandle::~IXFileHandle() {
}
bool IXFileHandle::isOpen() {
    return (bool)shared_item_;
}
RC IXFileHandle::setFile(FILE* fp){
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
        int roots[] = {-1, -1};
        fwrite(roots, sizeof(roots), 1, shared_item_->name);
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

int IXFileHandle::getRootPageNum(){
    fseek(shared_item_->name, 3 * sizeof(unsigned), SEEK_SET);
    int res;
    fread(&res, sizeof(int), 1, shared_item_->name);
    return res;
}
int IXFileHandle::setRootPageNum(int v){
    fseek(shared_item_->name, 3 * sizeof(unsigned), SEEK_SET);
    fwrite(&v, sizeof(int), 1, shared_item_->name);
    return 0;
}
int  IXFileHandle::getIdlePageNum(){
    fseek(shared_item_->name, 3 * sizeof(unsigned) + sizeof(int), SEEK_SET);
    int res;
    fread(&res, sizeof(int), 1, shared_item_->name);
    return res;
}
int  IXFileHandle::setIdlePageNum(int v){
    fseek(shared_item_->name, 3 * sizeof(unsigned) + sizeof(int), SEEK_SET);
    fwrite(&v, sizeof(int), 1, shared_item_->name);
    return 0;
}

/* ====================== IndexPage ==================== */
void IndexPage::InitializePage(char* page) {
    IndexPage::setSlotTableLen(page, 0);
    IndexPage::setStackTop(page, PAGE_SIZE);
    IndexPage::setNextLeafId(page, -1);
    IndexPage::setEmptySlotFlag(page, false);
}

void IndexPage::appendTailChildPointer(char* page){
    int childPageNum = -1;
    SlotItem slot;
    slot.data_size = 4;
    slot.field_num = 1;
    slot.metadata_size = 0;
    slot.offset = IndexPage::appendData(page, (char*)&childPageNum, sizeof(int));
    int i = IndexPage::appendSlot(page);
    IndexPage::writeSlot(page, i, slot);
}

void IndexPage::setSlotTableLen(char* page, TypeOffset n){
    *(TypeOffset*)page = n;
}

void IndexPage::setStackTop(char* page, TypeOffset n){
    *(TypeOffset*)(page + sizeof(TypeOffset)) = n;
}

void IndexPage::setNextLeafId(char* page, int n){
    *(int*)(page+2*sizeof(TypeOffset)) = n;
}

std::vector<char> IndexPage::readRawIndex(char* page, int i){
    if(i * sizeof(SlotItem) >= IndexPage::getSlotTableLen(page)){
        std::cerr << "readRawIndex error !!!" << std::endl;
        return std::vector<char>();
    }
    SlotItem& slotref = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    std::vector<char> res;
    int len = slotref.data_size - sizeof(int);
    pushBackTo(res, page + slotref.offset + sizeof(int), len);
    //if(len <= 0) std::cerr << "[readRawIndex] " << IndexPage::getSlotCount(page) << std::endl;
    return res;
}

IndexItem IndexPage::readIndexItem(char* page, int i){
    /*
    data format | leftChildPageNum | [key | rid] |
                                4B                                    x B    8 B
    */
     if(i * sizeof(SlotItem) >= IndexPage::getSlotTableLen(page)){
        std::cerr << "readIndexItem error !!!" << std::endl;
        return IndexItem();
    }
    SlotItem& slotref = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    IndexItem res;
    char* datastart = page + slotref.offset;
    res.leftChildPageNum = *(int*)datastart;
    datastart += sizeof(int);
    int len = slotref.data_size - sizeof(int) - sizeof(RID);
    //if(len < 4) 
    //    std::cerr << len <<" "<<i<<std::endl;
    pushBackTo(res.value, datastart, len);
    datastart += len;
    res.rid.pageNum = *(unsigned*)datastart;
    res.rid.slotNum = *(unsigned*)(datastart + sizeof(unsigned));
    return res;
}

void IndexPage::setChildPageNum(char* page, int childPageNum, int i) {
    SlotItem& slotref = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    *(int*)(page + slotref.offset) = childPageNum;
}

TypeOffset IndexPage::writeSlot(char* page, int slotidx, const SlotItem& slot) {
    memcpy(page + IndexPage::PAGEHEADSIZE + slotidx * sizeof(SlotItem), &slot, sizeof(slot));
    return slotidx;
}

TypeOffset IndexPage::writeData(char* page, TypeOffset offset, const char* data, int size) {
    if(offset < IndexPage::getStackTop(page)){
        IndexPage::setStackTop(page, offset);
    }
    memcpy(page + offset, data, size);
    return offset;
}

TypeOffset IndexPage::appendData(char* page,const char* data, int size){
    auto top = IndexPage::getStackTop(page);
    IndexPage::setStackTop(page, top - size);
    memcpy(page + top - size, data, size);
    return top - size;
}

int IndexPage::appendSlot(char* page){
    int slen = IndexPage::getSlotTableLen(page);
    IndexPage::setSlotTableLen(page, slen + sizeof(SlotItem));
    return slen / sizeof(SlotItem);
}

void IndexPage::compress(char* page, TypeOffset start, TypeOffset end, TypeOffset offset){
    // include range: [start, end]
    int len = end - start;
    char buf[len];
    memcpy(buf, page + start, len);
    memcpy(page + start + offset, buf, len);
}

void IndexPage:: insertValueTo(char* page, int childPageNum,std::vector<char>& value, int i) {
    if(i * sizeof(SlotItem) >= IndexPage::getSlotTableLen(page)){
        std::cerr << "insertValueTo error !!!" << std::endl;
        return;
    }
    int expectSlotNum = IndexPage::getSlotCount(page) + 1;
    // move index part
    SlotItem& startslot = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    SlotItem& endslot = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE +  IndexPage::getSlotTableLen(page) - sizeof(SlotItem));
    TypeOffset movestart = endslot.offset;
    TypeOffset moveend = startslot.offset + startslot.data_size;
    int len = value.size() + sizeof(childPageNum);
    IndexPage::compress(page, movestart, moveend, -len);
    IndexPage::setStackTop(page, movestart - len); // IMPORTANT
    // write index value
    IndexPage::writeData(page, moveend - len, (char*)&childPageNum, sizeof(childPageNum));
    IndexPage::writeData(page, moveend-len+sizeof(childPageNum), value.data(), value.size());
    SlotItem newslot{moveend - (TypeOffset)len, (TypeOffset)len, 0, 2};
    // move slot table part
    movestart = IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem);
    moveend = IndexPage::PAGEHEADSIZE + IndexPage::getSlotTableLen(page);
    IndexPage::compress(page, movestart, moveend, sizeof(SlotItem));
    // write new slot
    int numslot = IndexPage::getSlotCount(page) + 1;
    IndexPage::setSlotTableLen(page ,numslot * sizeof(SlotItem));
    IndexPage::writeSlot(page, i, newslot);
    // update offset of moved slot
    for(i = i + 1; i < numslot; ++ i){
        SlotItem& slotref = *(SlotItem*)(page + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        slotref.offset -= len;
    }
    // if(expectSlotNum != IndexPage::getSlotCount(page))
    //     std::cerr << "[Insert Error]" << expectSlotNum << " "<<  IndexPage::getSlotCount(page) << std::endl;
}

void IndexPage::insertValueAndSplitPage(char* oldpage, char* newpage, int childPageNum, std::vector<char>& value, int i){
    int slotnum = IndexPage::getSlotCount(oldpage);
    int space = PAGE_SIZE - IndexPage::getEmptySize(oldpage) + sizeof(int) + value.size() + sizeof(SlotItem);
    int spaceusage = 0;

    // linear search split index
    int j; bool split_before_i = false;
    for(j = 0; j < i; ++ j){
        SlotItem& tmpref = *(SlotItem*)(oldpage + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
        spaceusage += sizeof(SlotItem) + tmpref.data_size;
        if(spaceusage * 2 >= space ){
            split_before_i = true;
            break;
        }
    }
    // inserted slot
    SlotItem insertedSlot{0, sizeof(childPageNum) + value.size(), 0, 2};
    if(split_before_i) {
        // setting status for old page
        SlotItem& tmpref = *(SlotItem*)(oldpage + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
        TypeOffset new_oldtop = tmpref.offset, new_old_stablelen = (j + 1) * sizeof(SlotItem);
        IndexPage::setStackTop(oldpage, new_oldtop);
        IndexPage::setSlotTableLen(oldpage, new_old_stablelen);
        // copy remaining to new page
        IndexPage::InitializePage(newpage);
        // before i
        for(j = j + 1; j < i; ++ j){
            SlotItem tmp = *(SlotItem*)(oldpage + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
            tmp.offset = IndexPage::appendData(newpage, oldpage+tmp.offset, tmp.data_size);
            auto sidx = IndexPage::appendSlot(newpage);
            IndexPage::writeSlot(newpage, sidx, tmp);
        }
        // i
        IndexPage::appendData(newpage, value.data(), value.size());
        insertedSlot.offset = IndexPage::appendData(newpage, (char*)&childPageNum, sizeof(childPageNum));
        int insertedSidx = IndexPage::appendSlot(newpage);
        IndexPage::writeSlot(newpage, insertedSidx, insertedSlot);
        // after i
        for(; j < slotnum; ++ j){
            SlotItem tmp = *(SlotItem*)(oldpage + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
            tmp.offset = IndexPage::appendData(newpage, oldpage+tmp.offset, tmp.data_size);
            auto sidx = IndexPage::appendSlot(newpage);
            IndexPage::writeSlot(newpage, sidx, tmp);
        }
        // IMPORTANT: Don't forget this
        IndexPage::appendTailChildPointer(oldpage);
    } else {
        // backup old page
        char backup[PAGE_SIZE];
        memcpy(backup, oldpage, PAGE_SIZE);
        // write inserted Data
        SlotItem& tmpref = *(SlotItem*)(backup + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
        IndexPage::setStackTop(oldpage, tmpref.offset + tmpref.data_size);
        IndexPage::appendData(oldpage, value.data(), value.size());
        insertedSlot.offset = IndexPage::appendData(oldpage, (char*)&childPageNum, sizeof(childPageNum));
        IndexPage::writeSlot(oldpage, i, insertedSlot);
        spaceusage += sizeof(childPageNum) + value.size() + sizeof(SlotItem);
        for(; j < slotnum; ++ j){
            if(spaceusage * 2 >= space) break;
            SlotItem tmp = *(SlotItem*)(backup + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
            spaceusage += sizeof(SlotItem) + tmp.data_size;
            // append data
            tmp.offset = IndexPage::appendData(oldpage, backup+tmp.offset, tmp.data_size);
            IndexPage::writeSlot(oldpage, j+1, tmp);
        }
        IndexPage::setSlotTableLen(oldpage, (j + 1) * sizeof(SlotItem));
        IndexPage::appendTailChildPointer(oldpage);
        // write remaining to new page
         IndexPage::InitializePage(newpage);
        for(; j < slotnum; ++ j){
            SlotItem tmp = *(SlotItem*)(backup + IndexPage::PAGEHEADSIZE + j * sizeof(SlotItem));
            tmp.offset = IndexPage::appendData(newpage, backup+tmp.offset, tmp.data_size);
            auto sidx = IndexPage::appendSlot(newpage);
            IndexPage::writeSlot(newpage, sidx, tmp);
        }
    }
    int sn1 = IndexPage::getSlotCount(oldpage), sn2 = IndexPage::getSlotCount(newpage);
    // if(sn1 + sn2 != slotnum + 2)
    //     std::cerr <<"[Insert Error] "<< sn1 << " "<<sn2 << " "<<split_before_i<<std::endl;
}

void IndexPage::deleteIndexFrom(char* pagebuf, int i){
    SlotItem& startslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
    SlotItem& endslot = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE +  IndexPage::getSlotTableLen(pagebuf) - sizeof(SlotItem));
    TypeOffset movestart = endslot.offset;
    TypeOffset moveend = startslot.offset;
    int len = startslot.data_size;
    IndexPage::compress(pagebuf, movestart, moveend, len);
    // change slot table part
    startslot.offset = 0;
    // set page metadata
    IndexPage::setStackTop(pagebuf, IndexPage::getStackTop(pagebuf) + len);
    // update offset of moved slot
    int numslot = IndexPage::getSlotCount(pagebuf);
    for(i += 1; i < numslot; ++ i){
        SlotItem& slotref = *(SlotItem*)(pagebuf + IndexPage::PAGEHEADSIZE + i * sizeof(SlotItem));
        slotref.offset += len;
    }
}
void IndexPage::setEmptySlotFlag(char* page, bool v){
    bool* pos = (bool*)(page + IndexPage::PAGEHEADSIZE - sizeof(bool));
    *pos = v;
}
bool IndexPage::hasEmptySlot(char* page){
    return *(bool*)(page + IndexPage::PAGEHEADSIZE - sizeof(bool));
}
void IndexPage::garbageSlotCollection(char* page) {
    if(!IndexPage::hasEmptySlot(page)) return;
    int slotnum = IndexPage::getSlotCount(page);
    int left = 0, moveoffset = 0;
    SlotItem* item_start = (SlotItem*)(page + IndexPage::PAGEHEADSIZE);
    for(int i = 0; i < slotnum; ++ i){
        if(item_start[i].metadata_size == -1){
            moveoffset += item_start[i].data_size;
        } else if(left < i){
            int movestart = item_start[i].offset, moveend = item_start[i].offset + item_start[i].data_size;
            IndexPage::compress(page, movestart, moveend, moveoffset);
            item_start[i].offset += moveoffset;
            item_start[left++] = item_start[i];
        } else left ++;
    }
    IndexPage::setSlotTableLen(page, left * sizeof(SlotItem));
    IndexPage::setEmptySlotFlag(page, false);
    IndexPage::setStackTop(page, item_start[left-1].offset);
}
TypeOffset IndexPage::getSlotTableLen(char* page){
    return *(TypeOffset *) page;
}

TypeOffset IndexPage::getStackTop(char* page){
    return *(TypeOffset *) (page + sizeof(TypeOffset));
}

int IndexPage::getNextLeafId(char* page){
    return *(int*)(page+2*sizeof(TypeOffset));
}

TypeOffset IndexPage::getEmptySize(char *page){
    return IndexPage::getStackTop(page) - IndexPage::getSlotTableLen(page) - IndexPage::PAGEHEADSIZE;

}
int IndexPage::getSlotCount(char* page){
    return IndexPage::getSlotTableLen(page) / sizeof(SlotItem);
}
