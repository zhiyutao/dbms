
#include "qe.h"
#include <cfloat>
void makeTableAttrName(std::string colName, std::string* tableName, std::string* attrName){
    std::string tmp;
    int i = 0;
    for(; i < colName.size(); ++ i){
        if(colName[i] == '.') break;
        tmp.push_back(colName[i]);
    }
    if(tableName != nullptr) *tableName = std::move(tmp);
    if(attrName != nullptr) *attrName = std::string(colName.begin() + i + 1, colName.end());
}
std::vector<std::vector<char>> decoupleFieldValues (void * data, std::vector<Attribute> attributes, int* size) {
    int read_offset = getIndicatorLen(attributes.size());
    std::vector<std::vector<char>> ans;
    char *cp = static_cast<char*>(data);
    ans.emplace_back();
    pushBackTo(ans[0], cp, read_offset); // null indicator
    for (int i = 0; i < attributes.size(); ++i) {
        if(testBit((char*)data, i)) {
            ans.emplace_back();
            continue;
        }
        std::vector<char> tmp;
        switch (attributes[i].type) {
            case TypeReal:
                pushBackTo(tmp, cp + read_offset, 4);
                read_offset+=4;
                break;
            case TypeInt:
                pushBackTo(tmp, cp + read_offset, 4);
                read_offset+=4;
                break;
            case TypeVarChar: {
                int varlen = *(int *) (cp+read_offset);
                read_offset+=4;
                pushBackTo(tmp, (char *) &varlen, 4);
                pushBackTo(tmp, cp + read_offset, varlen);
                read_offset+=varlen;
                break;
            }
        }
        ans.emplace_back(tmp);
    }
    if(size)
        *size = read_offset;
    return ans;
}

template <typename Num>
int compareNumber(Num a, Num b){
    if(a == b) return 0;
    else if(a < b) return -1;
    else return 1;
}

bool applyComp(CompOp compOp, const char* left, const char* right, const Attribute& recordDescriptor){
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

Filter::Filter(Iterator *input, const Condition &condition) {
    if(condition.bRhsIsAttr == true)
        return;
    if (condition.lhsAttr == "" && condition.op != NO_OP) // invalid condition
        return;
    this->input = input;
    this->condition = condition;
    input->getAttributes(attributes);
    for(int i = 0; i < attributes.size(); ++ i){
        if(attributes[i].name == condition.lhsAttr){
            lAttrIdx_ = i; break;
        }
    }
}

// ... the rest of your implementations go here
RC Filter::getNextTuple(void *data){
    if(lAttrIdx_ < 0) return QE_EOF;
    while(input->getNextTuple(data) != QE_EOF) {
        std::vector<std::vector<char>> ans = decoupleFieldValues(data, attributes);
        if(applyComp(condition.op, ans[lAttrIdx_+1].data(), (char*)condition.rhsValue.data, attributes[lAttrIdx_ + 1])) {
            return 0;
        }
    }
    return QE_EOF;
};

Project::Project(Iterator *input,                    // Iterator of input R
        const std::vector<std::string> &attrNames) {
    this->input = input;
    input->getAttributes(attributes);
    std::unordered_map<std::string, int> name2idx;
    for(int i = 0; i < attributes.size(); ++ i){
        name2idx[attributes[i].name] = i;
    }
    for(auto& s: attrNames){
        selected_idx_.push_back(name2idx[s]);
    }
};   // std::vector containing attribute names

RC Project::getNextTuple(void *data) {
    if(input->getNextTuple(databuf_) == QE_EOF)
        return QE_EOF;
    std::vector<std::vector<char>> ans = decoupleFieldValues(databuf_, attributes);
    // copy null indicator
    int indicator_len = getIndicatorLen(selected_idx_.size());
    bzero(data, indicator_len);
    for(int i = 0; i < selected_idx_.size(); ++ i){
        if(testBit(ans[0].data(), selected_idx_[i])) setBit((char*)data, i);
    }
    // copy data field
    for(int i = 0; i < selected_idx_.size(); ++ i){
        memcpy(data+indicator_len, ans[selected_idx_[i]+1].data(), ans[selected_idx_[i]+1].size());
        indicator_len += ans[selected_idx_[i]+1].size();
    }
    return 0;
};


BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, 
        const unsigned numPages):numPages_(numPages), leftIn_(leftIn), rightIn_(rightIn), cond_(condition) {
    if(!cond_.bRhsIsAttr || cond_.lhsAttr.size() == 0 || cond_.rhsAttr.size() == 0) return;
    leftIn_->getAttributes(leftAttrs_);
    rightIn_->getAttributes(rightAttrs_);
    for(int i = 0; i < leftAttrs_.size(); ++ i){
        if(leftAttrs_[i].name == cond_.lhsAttr){
            lJoinIdx_ = i; break;
        }
    }
    for(int i = 0; i < rightAttrs_.size(); ++ i) {
        if(rightAttrs_[i].name == cond_.rhsAttr){
            rJoinIdx_ = i; break;
        }
    }
};

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    attrs = leftAttrs_;
    attrs.insert(attrs.end(), rightAttrs_.begin(), rightAttrs_.end());
}

RC BNLJoin::getNextTuple(void *data){ 
    if(lJoinIdx_ == -1 || rJoinIdx_ == -1)
        return QE_EOF;
    // check if inner record is finished
    if(joinRecord(data))
        return 0;
    // new inner record
    while(1){
        if(rightIn_->getNextTuple(data) == QE_EOF){
            if(loadLeftAndHash(data) == 0) return QE_EOF; // load next numPage_
            rightIn_->setIterator(); // reset inner relation
            if(rightIn_->getNextTuple(data) == QE_EOF) return QE_EOF; // read again
        }
        // reset inner record state
        currRightValues = decoupleFieldValues(data, rightAttrs_);
        innerLoopIdx = 0;
        if(joinRecord(data)) // successful call
            return 0;
    }
    return QE_EOF;
}

bool BNLJoin::joinRecord(void* data) {
    if(innerLoopIdx < 0) return false; // uninitialized call
    std::string key(currRightValues[rJoinIdx_+1].begin(), currRightValues[rJoinIdx_+1].end());
    if(hashmap_.count(key) == 0 || innerLoopIdx == hashmap_[key].size()) return false;
    
    // copy nullindicator
    auto& leftValue = hashmap_[key][innerLoopIdx];
     int indicator_len = getIndicatorLen(leftAttrs_.size() + rightAttrs_.size());
     bzero(data, indicator_len);
    memcpy(data, leftValue[0].data(), leftValue[0].size());
    for(int i = 0; i < rightAttrs_.size(); ++ i){
        if(testBit(currRightValues[0].data(), i)) setBit((char*)data, i +  leftAttrs_.size());
    }
    // copy left values
    for(int i = 1; i < leftValue.size(); ++ i){
        memcpy(data+indicator_len, leftValue[i].data(), leftValue[i].size());
        indicator_len += leftValue[i].size();
    }
    // copy right values
    for(int i = 1; i < currRightValues.size(); ++ i){
        memcpy(data+indicator_len, currRightValues[i].data(), currRightValues[i].size());
        indicator_len += currRightValues[i].size();
    }
    innerLoopIdx ++;
    return true;
}

int BNLJoin::loadLeftAndHash(void* data){ // load numPage_ record from outer relation
    currLoadSize_ = 0;
    const int threshold = PAGE_SIZE * numPages_;
    hashmap_.clear();
    while(leftIn_->getNextTuple(data) != QE_EOF && currLoadSize_ < threshold){ // approximate size
        int size;
        auto tmp = decoupleFieldValues(data, leftAttrs_, &size);
        hashmap_[
            std::string(tmp[lJoinIdx_+1].begin(), tmp[lJoinIdx_+1].end()) // hash key
            ].emplace_back(tmp);
        currLoadSize_ += size;
    } 
    return currLoadSize_;
}

void INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    attrs = leftAttrs_;
    attrs.insert(attrs.end(), rightAttrs_.begin(), rightAttrs_.end());
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition): 
    leftIn_(leftIn), rightIn_(rightIn), cond_(condition){
    
    if(!cond_.bRhsIsAttr || cond_.lhsAttr.size() == 0 || cond_.rhsAttr.size() == 0) return;
    leftIn_->getAttributes(leftAttrs_);
    rightIn_->getAttributes(rightAttrs_);
    for(int i = 0; i < leftAttrs_.size(); ++ i){
        if(leftAttrs_[i].name == cond_.lhsAttr){
            lJoinIdx_ = i; break;
        }
    }
}

RC INLJoin::getNextTuple(void *data) { 
    if(lJoinIdx_ < 0) return QE_EOF;
    if(joinRecord(data)) return 0;
    while(1){
        if(leftIn_->getNextTuple(data) == QE_EOF) // read next outer record
            return QE_EOF;
        currLeftValues = decoupleFieldValues(data, leftAttrs_);
        // reset Index iterator
        auto &key = currLeftValues[lJoinIdx_+1];
        rightIn_->setIterator(key.data(), key.data(), true, true); // only eq join ? 
        if(joinRecord(data)) return 0;
    }
    return QE_EOF;
}

bool INLJoin::joinRecord(void* data) {
    if(currLeftValues.size() == 0) return false; // uninitialized
    if(rightIn_->getNextTuple(data) == QE_EOF) return false;
    auto rightValues = decoupleFieldValues(data, rightAttrs_);
    // copy null indicator
    int indicator_len = getIndicatorLen(leftAttrs_.size() + rightAttrs_.size());
    bzero(data, indicator_len);
    memcpy(data, currLeftValues[0].data(), currLeftValues[0].size());
    for(int i = 0; i < rightAttrs_.size(); ++ i){
        if(testBit(rightValues[0].data(), i)) 
            setBit((char*)data, i + leftAttrs_.size());
    }

    for(int i = 1; i < currLeftValues.size(); ++ i){
        memcpy(data+indicator_len, currLeftValues[i].data(), currLeftValues[i].size());
        indicator_len += currLeftValues[i].size();
    }
    // copy right values
    for(int i = 1; i < rightValues.size(); ++ i){
        memcpy(data+indicator_len, rightValues[i].data(), rightValues[i].size());
        indicator_len += rightValues[i].size();
    }
    return true;
}

RC Aggregate::getCount(void* data){
    float count = 0;
    char buf[PAGE_SIZE];
    while(input_->getNextTuple(buf) != QE_EOF)
        count ++;

    // skip null indicator, so plus one
    memcpy(data+1, &count, sizeof(float));
    finish_ = true;
    return 0;
}

RC Aggregate::getMax(void *data) {
    if(aggAttr_.type == TypeVarChar) return QE_EOF;
    char buf[PAGE_SIZE];
    float max = FLT_MIN;
    if(aggAttr_.type == TypeInt) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            if (max < *(int *) ans[AttrIdx_ + 1].data()) {
                max = (float)(*(int *) ans[AttrIdx_ + 1].data());
            }
        }
        memcpy(data+1, &max, sizeof(int));
        finish_ = true;
        return 0;
    }
    else if(aggAttr_.type == TypeReal) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            if (max < *(float *) ans[AttrIdx_ + 1].data()) {
                max = *(float *) ans[AttrIdx_ + 1].data();
            }
        }
        memcpy(data+1, &max, sizeof(float));
        finish_ = true;
        return 0;
    }
    return QE_EOF;
}

RC Aggregate::getMin(void *data) {
    if(aggAttr_.type == TypeVarChar) return QE_EOF;
    char buf[PAGE_SIZE];
    float min = FLT_MAX;
    if(aggAttr_.type == TypeInt) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            if (min > *(int *) ans[AttrIdx_ + 1].data()) {
                min = (float)(*(int *) ans[AttrIdx_ + 1].data());
            }
        }
        memcpy(data+1, &min, sizeof(int));
        finish_ = true;
        return 0;
    }
    else if(aggAttr_.type == TypeReal) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            if (min > *(float *) ans[AttrIdx_ + 1].data()) {
                min = *(float *) ans[AttrIdx_ + 1].data();
            }
        }
        memcpy(data+1, &min, sizeof(float));
        finish_ = true;
        return 0;
    }
    return QE_EOF;
}

RC Aggregate::getSum(void *data) {
    if(aggAttr_.type == TypeVarChar) return QE_EOF;
    char buf[PAGE_SIZE];
    float sum = 0;

    if(aggAttr_.type == TypeInt) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            sum += (float)(*(int *) ans[AttrIdx_ + 1].data());
        }
        memcpy(data+1, &sum, sizeof(int));
        finish_ = true;
        return 0;
    }
    else if(aggAttr_.type == TypeReal) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            sum += *(float *) ans[AttrIdx_ + 1].data();
        }
        memcpy(data+1, &sum, sizeof(float));
        finish_ = true;
        return 0;
    }
    return QE_EOF;
}

RC Aggregate::getAvg(void *data) {
    if(aggAttr_.type == TypeVarChar) return QE_EOF;
    char buf[PAGE_SIZE];
    float sum = 0;
    int count = 0;
    if(aggAttr_.type == TypeInt) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            sum += (float)(*(int *) ans[AttrIdx_ + 1].data());
            count ++;
        }
        float avg = sum/(count*1.0);
        memcpy(data+1, &avg, sizeof(float));
        finish_ = true;
        return 0;
    }
    else if(aggAttr_.type == TypeReal) {
        while (input_->getNextTuple(buf) != QE_EOF) {
            std::vector<std::vector<char>> ans = decoupleFieldValues(buf, attributes);
            sum += *(float *) ans[AttrIdx_ + 1].data();
            count ++;
        }
        float avg = sum/count;
        memcpy(data+1, &avg, sizeof(float));
        finish_ = true;
        return 0;
    }
    return QE_EOF;
}

Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op): 
    input_(input), aggAttr_(aggAttr), aggOp_(op) {
    input->getAttributes(attributes);
    for(int i = 0; i < attributes.size(); ++ i){
        if(attributes[i].name == aggAttr.name){
            AttrIdx_ = i; break;
        }
    }
}

RC Aggregate::getNextTuple(void *data){
    if(finish_ || aggAttr_.type == TypeVarChar) return QE_EOF;
    switch(aggOp_) {
        case AggregateOp::COUNT:
            return getCount(data);
        case AggregateOp::MAX:
            return getMax(data);
        case AggregateOp::MIN:
            return getMin(data);
        case AggregateOp::SUM:
            return getSum(data);
        case AggregateOp::AVG:
            return getAvg(data);
        default: return -1;
    }
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
    Attribute res;
    res.type = TypeReal;
    res.length = 4;
    if(aggOp_ == AggregateOp::COUNT) {
        res.name = "COUNT("+aggAttr_.name+")";
    } 
    // TODO: other aggregator
    else if (aggOp_ == AggregateOp::MAX) {
        res.name = "MAX("+aggAttr_.name+")";
    }
    else if (aggOp_ == AggregateOp::MIN) {
        res.name = "MIN("+aggAttr_.name+")";
    }
    else if (aggOp_ == AggregateOp::SUM) {
        res.name = "SUM("+aggAttr_.name+")";
    }
    else if (aggOp_ == AggregateOp::AVG) {
        res.name = "AVG("+aggAttr_.name+")";
    }
    else return;
    attrs.emplace_back(res);
    return;
}