#include "src/include/rbfm.h"
#include "src/include/pfm.h"

#include <iostream>
#include <cmath>

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        int slotOffset = 0;
        int ibuffer = 0;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char* nullbits;
        nullbits = (unsigned char*)malloc(nullField);
        memcpy(nullbits, (char*)data + slotOffset, nullField);
        slotOffset += nullField;

        for(int i=0; i < recordDescriptor.size();i++)
        {
            bool nullBit = nullbits[int(floor((double) i/8))] & ((unsigned) 1 << (unsigned) (7-(i%8)));
            if(!nullBit)
            {
                switch (recordDescriptor[i].type) {
                    case 0:
                        slotOffset += sizeof(int);
                        break;
                    case 1:
                        slotOffset += sizeof(float);
                        break;
                    case 2:
                        ibuffer = 0;
                        memcpy(&ibuffer, (char *) data + slotOffset, sizeof(int));
                        slotOffset += sizeof(int);
                        slotOffset += ibuffer;
                        break;
                }
            }
        }

        int recordOffsetField = 4092;
        int RIDOffsetFields = 4088;

        void * pageData;
        pageData = malloc(4096);

        unsigned readCount = 0;
        unsigned writeCount = 0;
        unsigned appendCount = 0;

        fileHandle.collectCounterValues(readCount,writeCount,appendCount);

        //create new page if freshfile (I am creating a duplicate of this for time being for when a new page
        // is needed in an existing file, will optimize later)
        if(appendCount <= 0)
        {
//            for(int i = 0; i < 1024; i++)
//                memcpy((char *) pageData + (i*sizeof(int)), &recordOffsetField, sizeof(int));
            memcpy((char *) pageData, (char *) data, slotOffset);

            //slotOffset is length of the record so:
            int slotbegin=0;
            int RIDOffsetvalue = 4076;
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(unsigned)*3), &nullField, sizeof(int));
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(unsigned)*2), &slotOffset, sizeof(int));
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(unsigned)), &slotbegin, sizeof(int));

            memcpy((char *) pageData + RIDOffsetFields, &RIDOffsetvalue, sizeof(int));
            memcpy((char *) pageData + recordOffsetField, &slotOffset, sizeof(int));

            rid.pageNum = 0;
            rid.slotNum = RIDOffsetFields-sizeof(unsigned);

            ibuffer = fileHandle.appendPage(pageData);

//            for(int j =0; j<25;j++)
//            {
//                memcpy(&slotOffset, (char *) pageData +j, sizeof(int));
//                std::cout<< j<< ":" << slotOffset << "\n";
//            }

            return ibuffer;
        }
        else
        {
            int totalRecordOffsets = 0;
            int totalRIDOffsets = 0;
            for(int i=0; i < appendCount; i++)
            {
                fileHandle.readPage(i, pageData);

//                for(int j =0; j<4096;j++)
//                {
//                    memcpy(&totalRecordOffsets, (char *) pageData +j, sizeof(int));
//                    std::cout<< j<< ":" << totalRecordOffsets << "\n";
//                }
//                throw std::invalid_argument("I want to cry");

                memcpy(&totalRecordOffsets, (char *) pageData + recordOffsetField, sizeof(int));
                memcpy(&totalRIDOffsets, (char *) pageData + RIDOffsetFields, sizeof(int));

                if((totalRIDOffsets-totalRecordOffsets) > (slotOffset + (sizeof(int)*4)))
                {
                    //set rids
                    rid.pageNum = i;
                    rid.slotNum = totalRIDOffsets-sizeof(int);

                    //add new data
                    memcpy((char *) pageData + totalRecordOffsets, (char *) data, slotOffset);

                    //add new record feild in page
                    memcpy((char *) pageData + (totalRIDOffsets-sizeof(int)), &totalRecordOffsets, sizeof(int));
                    memcpy((char *) pageData + (totalRIDOffsets-sizeof(int)*2), &slotOffset, sizeof(int));
                    nullField += totalRecordOffsets;
                    memcpy((char *) pageData + (totalRIDOffsets-sizeof(int)*3), &nullField, sizeof(int));

                    //update offset values at end of page
                    totalRecordOffsets += slotOffset;
                    totalRIDOffsets -= 3*sizeof(int);
                    memcpy((char *) pageData + recordOffsetField, &totalRecordOffsets, sizeof(unsigned));
                    memcpy((char *) pageData + RIDOffsetFields, &totalRIDOffsets, sizeof(unsigned ));


                    ibuffer = fileHandle.writePage(i, pageData);
                    return ibuffer;
                }
            }

            //if here no page is large enough
            memcpy((char *) pageData, (char *) data + slotOffset, slotOffset);

            //slotOffset is length of the record so:
            int slotbegin=0;
            int RIDOffsetvalue = 4076;
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(int)), &slotbegin, sizeof(int));
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(int)*2), &slotOffset, sizeof(int));
            memcpy((char *) pageData + (RIDOffsetFields-sizeof(int)*3), &nullField, sizeof(int));
            memcpy((char *) pageData + RIDOffsetFields, &RIDOffsetvalue, sizeof(int ));
            memcpy((char *) pageData + recordOffsetField, &slotOffset, sizeof(int));

            ibuffer = fileHandle.appendPage(pageData);
            if (ibuffer == -1)
                return ibuffer;

            fileHandle.collectCounterValues(readCount,writeCount,appendCount);

            rid.pageNum = appendCount;
            rid.slotNum = RIDOffsetFields-sizeof(int);

            return ibuffer;
        }

        return -1;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        void * pageData;
        pageData = malloc(4096);

        if(fileHandle.readPage(rid.pageNum, pageData) == -1)
            return -1;

//        int v = 0;
//        for(int j =0; j<25;j++)
//        {
//            memcpy(&v, (char *) pageData +j, sizeof(int));
//            std::cout<< j<< ":" << v << "\n";
//        }

        int recordOffset = 0; //for nullfield
        int pageOffset = 0;
        int ibuffer = 0;

        //get nullfield
        memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char* nullbits;
        nullbits = (unsigned char*)malloc(nullField);
        memcpy(nullbits, (char*)pageData + pageOffset, nullField);
        memcpy((char*)data + recordOffset, (char*)pageData + pageOffset, nullField);
        pageOffset += nullField;
        recordOffset += nullField;

        for(int i=0; i < recordDescriptor.size();i++)
        {
            bool nullBit = nullbits[int(floor((double) i/8))] & ((unsigned) 1 << (unsigned) (7-(i%8)));
            if(!nullBit) {
                switch (recordDescriptor[i].type) {
                    case 0:
                        memcpy((char *) data + recordOffset, (char *) pageData + pageOffset, sizeof(int));
                        recordOffset += sizeof(int);
                        pageOffset += sizeof(int);
                        break;
                    case 1:
                        memcpy((char *) data + recordOffset, (char *) pageData + pageOffset, sizeof(float));
                        recordOffset += sizeof(float);
                        pageOffset += sizeof(float);
                        break;
                    case 2:
                        ibuffer = 0;
                        memcpy(&ibuffer, (char *) pageData + pageOffset, sizeof(int));
                        memcpy((char *) data + recordOffset, &ibuffer, sizeof(int));
                        recordOffset += sizeof(int);
                        pageOffset += sizeof(int);

                        memcpy((char *) data + recordOffset, (char *) pageData + pageOffset, ibuffer);
                        recordOffset += ibuffer;
                        pageOffset += ibuffer;
                        break;
                }
            }
        }

        //free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int offset = 0;// nullField; //for nullfield
        int ibuffer = 0;
        float fbuffer = 0.0;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        int v = 0;
        for(int j =0; j<25;j++)
        {
            memcpy(&v, (char *) data +j, sizeof(int));
            std::cout<< j<< ":" << v << "\n";
        }

//        std::cout<< nullField<<"\n";
        unsigned char* nullbits;
        nullbits = (unsigned char*)malloc(nullField);
        memcpy(&nullbits, (char*)data + offset, nullField);
        offset += nullField;

        for(int i=0; i < recordDescriptor.size();i++)
        {
//            int kk = nullbits[int(floor((double) i/8))];
            bool nullBit = nullbits[int(floor((double) i/8))] & ((unsigned) 1 << (unsigned) (7-(i%8)));
            if(nullBit)
            {
                out << recordDescriptor[i].name << ":NULL, ";
            }
            else {
                //std::cout << recordDescriptor[i].type << "\n";
                switch (recordDescriptor[i].type) {
                    case 0:

                        ibuffer = 0;
                        memcpy(&ibuffer, (char *) data + offset, sizeof(int));
                        offset += sizeof(int);
                        out << recordDescriptor[i].name << ": " << ibuffer << ", ";
                        break;
                    case 1:
                        fbuffer = 0.0;
                        memcpy(&fbuffer, (char *) data + offset, sizeof(float));
                        offset += sizeof(float);
                        out << recordDescriptor[i].name << ": " << fbuffer << ", ";
                        break;
                    case 2:
                        ibuffer = 0;
                        void *sbuffer;

                        memcpy(&ibuffer, (char *) data + offset, sizeof(int));
                        offset += sizeof(int);

                        std::string tempStr((char *) data + offset);
                        std::string useStr = tempStr.substr(0, ibuffer);
                        //memcpy((char *) sbuffer, (char *) data + offset, ibuffer);
                        offset += ibuffer;
                        //std::string tempStr((char *) sbuffer);
                        //std::string useStr = tempStr.substr(0, ibuffer);

                        //std::cout << recordDescriptor[i].name << ":" << useStr << ", " ;
                        out << recordDescriptor[i].name << ":" << useStr << ", ";
                        break;
                }
            }
            out << "\n";
        }
        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {



        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

