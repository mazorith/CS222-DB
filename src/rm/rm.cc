#include "src/include/rm.h"

#include <cmath>
namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() = default;

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        rbfm = &RecordBasedFileManager::instance();
        //tables names should be the same as table file names
        Attribute attr;

        //attrs for table-table
        if(table_attrs.size() == 0)
        {
            attr.name = "table-id";
            attr.type = AttrType::TypeInt;
            attr.length = sizeof(int);
            table_attrs.push_back(attr);

            attr.name = "table-name";
            attr.type = AttrType::TypeVarChar;
            attr.length = 50;
            table_attrs.push_back(attr);

            attr.name = "file-name";
            attr.type = AttrType::TypeVarChar;
            attr.length = 50;
            table_attrs.push_back(attr);
        }

        //attrs for column table
        if(column_attrs.size() == 0)
        {
            attr.name = "table-id";
            attr.type = AttrType::TypeInt;
            attr.length = sizeof(int);
            column_attrs.push_back(attr);

            attr.name = "column-name";
            attr.type = AttrType::TypeVarChar;
            attr.length = 50;
            column_attrs.push_back(attr);

            attr.name = "column-type";
            attr.type = AttrType::TypeInt;
            attr.length = sizeof(int);
            column_attrs.push_back(attr);

            attr.name = "column-length";
            attr.type = AttrType::TypeInt;
            attr.length = sizeof(int);
            column_attrs.push_back(attr);

            attr.name = "column-position";
            attr.type = AttrType::TypeInt;
            attr.length = sizeof(int);
            column_attrs.push_back(attr);
        }

        //========================================================================================================
        //create tables table and populate
        std::string file_name = "Tables";
        std::string name = "Tables";
        if(rbfm->createFile(file_name) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(file_name,fileHandle) == -1)
            return -1;

//        int nullFieldsIndicatorActualSize = ceil((double) table_attrs.size() / 8);
//        auto indicator = new unsigned char[nullFieldsIndicatorActualSize];
//        memset(indicator, 0, nullFieldsIndicatorActualSize);
        unsigned nullthing = 0;

        int table_id = 1;
        size_t name_length = name.length();
        int offset = 0;

        RID table_rid;
        void* tupleForTables;
        tupleForTables = malloc(100);

        memcpy((char*)tupleForTables, &nullthing, sizeof(unsigned));
        offset += 1;//sizeof(unsigned);
        memcpy((char*)tupleForTables+offset, &table_id, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, name.c_str(), name_length);
        offset += name_length;
        memcpy((char*)tupleForTables+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, name.c_str(), name_length);
        offset += name_length;

        if(rbfm->insertRecord(fileHandle, table_attrs, tupleForTables, table_rid) == -1)
            return -1;

        table_id++;
        name = "Columns";
        name_length = name.length();
        offset = 0;

        memcpy((char*)tupleForTables, &nullthing, sizeof(unsigned));
        offset += 1;//sizeof(unsigned);
        memcpy((char*)tupleForTables+offset, &table_id, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, name.c_str(), name_length);
        offset += name_length;
        memcpy((char*)tupleForTables+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tupleForTables+offset, name.c_str(), name_length);
        offset += name_length;

        if(rbfm->insertRecord(fileHandle, table_attrs, tupleForTables, table_rid) == -1)
            return -1;

        if(rbfm->closeFile(fileHandle) == -1)
            return -1;

        tablecount ++;

        //===========================================================================================================
        //create columns table and populate
        file_name = "Columns";
        name = "Columns";

        fileHandle.readPageCounter=0;
        fileHandle.writePageCounter=0;
        fileHandle.appendPageCounter=0;
        fileHandle.totalPages=0;

        if(rbfm->createFile(file_name) == -1)
            return -1;

        if(rbfm->openFile(file_name,fileHandle) == -1)
            return -1;

//        int nullFieldsIndicatorActualSize = ceil((double) column_attrs.size() / 8);
//        auto indicator2 = new unsigned char[nullFieldsIndicatorActualSize];
//        memset(indicator2, 0, nullFieldsIndicatorActualSize);

        table_id = 1;
        int position = 1;

        RID column_rid;
        void* tupleForColumns;
        tupleForColumns = malloc(100);


        for(int i =0; i < table_attrs.size(); i++)
        {
            offset = 0;
            name_length = table_attrs[i].name.length();

            memcpy((char*)tupleForColumns, &nullthing, sizeof(unsigned));
            offset += 1;//sizeof(unsigned);
            memcpy((char*)tupleForColumns+offset, &table_id, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &name_length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, table_attrs[i].name.c_str(), name_length);
            offset += name_length;

            memcpy((char*)tupleForColumns+offset, &attrConvert[table_attrs[i].type], sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &table_attrs[i].length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &position, sizeof(int));
            offset += sizeof(int);

            if(rbfm->insertRecord(fileHandle, column_attrs, tupleForColumns, column_rid) == -1)
                return -1;

            position++;
        }

        table_id++;
        position = 1;
        for(int i =0; i < column_attrs.size(); i++)
        {
            offset = 0;
            name_length = column_attrs[i].name.length();

            memcpy((char*)tupleForColumns, &nullthing, sizeof(unsigned));
            offset += 1;//sizeof(unsigned);
            memcpy((char*)tupleForColumns+offset, &table_id, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &name_length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, column_attrs[i].name.c_str(), name_length);
            offset += name_length;

            memcpy((char*)tupleForColumns+offset, &attrConvert[column_attrs[i].type], sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &column_attrs[i].length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tupleForColumns+offset, &position, sizeof(int));
            offset += sizeof(int);

            if(rbfm->insertRecord(fileHandle, column_attrs, tupleForColumns, column_rid) == -1)
                return -1;

            position++;
        }

        if(rbfm->closeFile(fileHandle) == -1)
            return -1;

        tablecount++;
        has_catalog = true;
        return 0;
    }

    RC RelationManager::deleteCatalog() {
        if (!has_catalog)
            return -1;

        //deleteTable("Tables");
        //deleteTable("Columns");
        if(rbfm->destroyFile("Tables") == -1)
            return -1;
        if(rbfm->destroyFile("Columns") == -1)
            return -1;
        has_catalog = false;

        return 0;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if(!has_catalog)
            return -1;

        if(rbfm->createFile(tableName) == -1)
            return -1;

        FileHandle fileHandle;

        //create entry in tables table
        if(rbfm->openFile("Tables",fileHandle) == -1)
            return -1;

        unsigned indicator = 0;

        int table_id = tablecount+1;
        int name_length = tableName.length();

        RID rid;
        void* tuple;
        tuple = malloc(200);
        int offset = 0;

        memcpy((char*)tuple, &indicator, sizeof(unsigned));
        offset += 1;//sizeof(unsigned);
        memcpy((char*)tuple+offset, &table_id, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tuple+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tuple+offset, tableName.c_str(), name_length);
        offset += name_length;
        memcpy((char*)tuple+offset, &name_length, sizeof(int));
        offset += sizeof(int);
        memcpy((char*)tuple+offset, tableName.c_str(), name_length);
        offset += name_length;

        if(rbfm->insertRecord(fileHandle, table_attrs, tuple, rid) == -1)
            return -1;

        if (rbfm->closeFile(fileHandle) == -1)
            return -1;

        //create entry in columns table
        if(rbfm->openFile("Columns",fileHandle) == -1)
            return -1;

        int position = 1;
        for(int i =0; i < attrs.size(); i++)
        {
            offset = 0;
            name_length = attrs[i].name.length();

            memcpy((char*)tuple, &indicator, sizeof(unsigned));
            offset += 1;
            memcpy((char*)tuple+offset, &table_id, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tuple+offset, &name_length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tuple+offset, attrs[i].name.c_str(), name_length);
            offset += name_length;

            memcpy((char*)tuple+offset, &attrConvert[attrs[i].type], sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tuple+offset, &attrs[i].length, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)tuple+offset, &position, sizeof(int));
            offset += sizeof(int);

            if(rbfm->insertRecord(fileHandle, column_attrs, tuple, rid) == -1)
                return -1;

            position++;
        }

        if (rbfm->closeFile(fileHandle) == -1)
            return -1;

        tablecount++;
        return 0;

//        std::vector<unsigned> types_and_lengths;
//        std::vector<std::string> names;
//        types_and_lengths.push_back(unsigned(attrs.size()));
//
//        fileHandle.masterAttributeCount++;
//        for(int i = 0; i<attrs.size(); i++)
//        {
//            types_and_lengths.push_back(unsigned(attrs[i].type));
//            types_and_lengths.push_back(unsigned(attrs[i].length));
//            names.push_back(attrs[i].name);
//        }
//
//        fileHandle.attributes.push_back(types_and_lengths);
//        fileHandle.attribute_names.push_back(names);
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if(tableName == "Tables" || tableName == "Columns")
            return -1;

        if(rbfm->destroyFile(tableName) == -1)
            return -1;

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        //scan table to get table_id
        int table_id = 0;
        int stop_condition = 0;

        void *tuple;
        tuple = malloc(100);
        RID rid;
        rid.pageNum = 0;
        rid.slotNum = 4084;
        int name_offset = 5;
        std::string test_string = "";
        int ibuffer = 0;

        RM_ScanIterator iterator = RM_ScanIterator();
        FileHandle fileHandle;
        if(rbfm->openFile("Tables",fileHandle) == -1)
            return -1;

        iterator.fileHandle = &fileHandle;

        bool found_table = false;
        while(stop_condition != RM_EOF && !found_table)
        {
            test_string = "";
            stop_condition = iterator.getNextTuple(rid, tuple);
            if(stop_condition != RM_EOF)
            {
                memcpy(&ibuffer, (char*)tuple+name_offset, sizeof(int));
                test_string.resize(ibuffer);
                memcpy((char*)test_string.data(), (char*)tuple+name_offset+sizeof(int), ibuffer);

                if(test_string == tableName)
                {
                    memcpy(&table_id, (char*)tuple+1, sizeof(int));
                    found_table = true;
                }
            }
        }

        if(rbfm->closeFile(fileHandle) == -1)
            return -1;

        if(table_id == 0) //table not found
            return -1;

        //scan columns where table_id == id
        rid.pageNum = 0;
        rid.slotNum = 4084;

        if(rbfm->openFile("Columns",fileHandle) == -1)
            return -1;

        std::string column_str = "Columns";
        fileHandle.file = column_str.c_str();
        iterator.fileHandle = &fileHandle;
        iterator.pageData_collected = false;

        stop_condition = 0;
        bool found_columns = false;
        Attribute attr;
        int value_holder = 0;
        while(stop_condition != RM_EOF && !found_columns)
        {
            ibuffer = 1;
            attr.name = "";
            attr.length = 4; //need to set this for some reason
            stop_condition = iterator.getNextTuple(rid, tuple);
            if(stop_condition != RM_EOF)
            {
                memcpy(&value_holder, (char*)tuple+1, sizeof(int));
                if(value_holder == table_id)
                {
                    ibuffer += sizeof(int);
                    memcpy(&value_holder, (char*)tuple+ibuffer, sizeof(int));
                    ibuffer += sizeof(int);
                    attr.name.resize(value_holder);
                    memcpy((char*)attr.name.data(), (char*)tuple+ibuffer, value_holder);
                    ibuffer += value_holder;

                    memcpy(&value_holder, (char*)tuple+ibuffer, sizeof(int));
                    attr.type = AttrType(value_holder);
                    ibuffer += sizeof(int);
                    memcpy(&attr.length, (char*)tuple+ibuffer, value_holder);

                    attrs.push_back(attr);
                }
                else if(value_holder > table_id)
                    found_columns = true;
            }
        }

        if(rbfm->closeFile(fileHandle) == -1)
            return -1;

        free(tuple);
        return 0;

//        Attribute attr;
//        //for now we will just grab the first set of attributes
//        int first_vector_offset = 1;
//        for(int i = 0; i < fileHandle.attribute_names[0].size(); i++)
//        {
//            attr.name = fileHandle.attribute_names[0][i];
//            attr.type = AttrType(fileHandle.attributes[0][i+first_vector_offset]);
//            first_vector_offset++;
//            attr.length = fileHandle.attributes[0][i+first_vector_offset];
//            first_vector_offset++;
//
//            attrs.push_back(attr);
//        }
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if(tableName == "Tables" || tableName == "Columns")
            return -1;

        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        if(rbfm->insertRecord(fileHandle,attrs,data,rid) == -1)
            return -1;

        return rbfm->closeFile(fileHandle);
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if(tableName == "Tables" || tableName == "Columns")
            return -1;

        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        if(rbfm->deleteRecord(fileHandle,attrs,rid) == -1)
            return -1;

        return rbfm->closeFile(fileHandle);
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if(tableName == "Tables" || tableName == "Columns")
            return -1;

        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        if(rbfm->updateRecord(fileHandle,attrs,data,rid) == -1)
            return -1;

        return rbfm->closeFile(fileHandle);
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        if(rbfm->readRecord(fileHandle,attrs,rid,data) == -1)
            return -1;

        return rbfm->closeFile(fileHandle);
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return rbfm->printRecord(attrs,data,out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        if(rbfm->readAttribute(fileHandle,attrs,rid,attributeName,data) == -1)
            return -1;

        return rbfm->closeFile(fileHandle);

//        return -1;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator)

    {
        std::vector<Attribute> attrs;
        if(getAttributes(tableName,attrs) == -1)
            return -1;

        FileHandle fileHandle;
        if(rbfm->openFile(tableName, fileHandle) == -1)
            return -1;

        RBFM_ScanIterator rbfm_ScanIterator;
        rbfm_ScanIterator.filename = tableName;
        rbfm->scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames,rbfm_ScanIterator);

        rm_ScanIterator.attributeNames = attributeNames;
        rm_ScanIterator.attrs = attrs;
        rm_ScanIterator.table_name = tableName;

        rm_ScanIterator.rbfm_ScanIterator = &rbfm_ScanIterator;
        rm_ScanIterator.rbfm = rbfm;

        rm_ScanIterator.scan_function = true;
        rm_ScanIterator.file_is_open = false;
        rm_ScanIterator.rids = rbfm_ScanIterator.rids;


        return rbfm->closeFile(fileHandle);
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data)
    {
        if(scan_function)
        {

            if(index >= rids.size())
            {
                return RM_EOF;
            }

            FileHandle fileh;
            bool opened = false;
            if(!file_is_open || rids[index].pageNum != current_page)
            {
                rbfm->openFile(table_name, fileh);
                //fileHandle->file = table_name.c_str();
                file_is_open = true;
                opened = true;
            }

            rbfm->custom_extract(fileh, attrs, attributeNames, current_page, rids[index], pageData, data);
            ++index;

            if(opened)
                rbfm->closeFile(fileh);
//            if(reset_values)
//            {
//                rbfm_ScanIterator->index = 0;
//                rbfm_ScanIterator->current_page = 0;
//            }
//
//            int return_val = rbfm_ScanIterator->getNextRecord(rid, data);
//            if (return_val == RBFM_EOF)
//            {
////                rbfm->closeFile(*fileHandle);
////                file_is_open = false;
////                free(pageData);
//                return RM_EOF;
//            }
//            else
//                return 0;

        }
        else
        {
            //for first rid collection
            if(!pageData_collected)
            {
                fileHandle->readPage(rid.pageNum, pageData);
                memcpy(&end_of_page, (char*)pageData+4088, sizeof(unsigned));
                pageData_collected = true;
            }
            while(end_of_page >= rid.slotNum)
            {
                if(fileHandle->appendPageCounter-1 >= rid.pageNum)
                {
                    free(pageData);
                    return RM_EOF;
                }

                rid.pageNum += 1;
                rid.slotNum = 4084;
                fileHandle->readPage(rid.pageNum, pageData);
                end_of_page = 0;
                memcpy(&end_of_page, (char*)pageData+4088, sizeof(unsigned));
            }

            unsigned begin_offset = 0;
            unsigned end_offset = 0;

            memcpy(&begin_offset, (char*)pageData+rid.slotNum, sizeof(int));
            memcpy(&end_offset, (char*)pageData+rid.slotNum-sizeof(int), sizeof(int));

            memcpy((char*)data, (char*)pageData+begin_offset, end_offset-begin_offset);

            //note sure exactly how we are supposed to use this so just assuming for RM implementation we only have 1 RID
            rid.slotNum -= 12;
        }

        return 0;
    }

    RC RM_ScanIterator::close()
    {
        return 0;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB