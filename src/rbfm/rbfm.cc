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

        unsigned char nullbits[nullField];
//        nullbits = (unsigned char*)malloc(nullField);
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

            free(pageData);
            return ibuffer;
        }
        else
        {
            int totalRecordOffsets = 0;
            int totalRIDOffsets = 0;
            if (slotOffset <= 2000){
                for (int i = -1; i+1 < appendCount+1; i++) {
                    if (i == -1)
                        fileHandle.readPage(appendCount - 1, pageData);
                    else
                        fileHandle.readPage(i, pageData);

                    memcpy(&totalRecordOffsets, (char *) pageData + recordOffsetField, sizeof(int));
                    memcpy(&totalRIDOffsets, (char *) pageData + RIDOffsetFields, sizeof(int));

                    if ((totalRIDOffsets - totalRecordOffsets) > (slotOffset + (sizeof(int) * 4))) {
                        //set rids
                        if (i == -1)
                            rid.pageNum = appendCount - 1;
                        else
                            rid.pageNum = i;

                        //check if there are empty slot ids to reuse
                        bool found_empty = false;
                        int RID_offset = totalRIDOffsets + sizeof(int) * 2;
                        rid.slotNum = totalRIDOffsets - sizeof(int);
                        for (int j = RID_offset; j < RIDOffsetFields; j += sizeof(int) * 3) {
                            memcpy(&RID_offset, (char *) pageData + j, sizeof(int));

                            if (RID_offset == -1) {
                                rid.slotNum = j;
                                found_empty = true;
                            }
                        }

                        //add new data
                        memcpy((char *) pageData + totalRecordOffsets, (char *) data, slotOffset);

                        //add new record feild in page
                        memcpy((char *) pageData + (rid.slotNum), &totalRecordOffsets, sizeof(int)); //begin
                        totalRecordOffsets += slotOffset;
                        memcpy((char *) pageData + (rid.slotNum - sizeof(int)), &totalRecordOffsets, sizeof(int)); //end
                        memcpy((char *) pageData + (rid.slotNum - sizeof(int) * 2), &nullField,
                               sizeof(int)); //nullfield size

                        //update offset values at end of page
                        memcpy((char *) pageData + recordOffsetField, &totalRecordOffsets, sizeof(unsigned));

                        //if we found an empty slot no need to update the rid offset field with a new rid slot
                        if (!found_empty) {
                            totalRIDOffsets -= 3 * sizeof(int);
                            memcpy((char *) pageData + RIDOffsetFields, &totalRIDOffsets, sizeof(unsigned));
                        }

                        if (i == -1)
                            ibuffer = fileHandle.writePage(appendCount - 1, pageData);
                        else
                            ibuffer = fileHandle.writePage(i, pageData);
                        free(pageData);
                        return ibuffer;
                    }
                }
            }
            //if here no page is large enough overwrite the copy of the previous page
            memcpy((char *) pageData, (char *) data, slotOffset);

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

            rid.pageNum = appendCount-1;
            rid.slotNum = RIDOffsetFields-sizeof(int);

            free(pageData);
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

        int recordOffset = 0; //for nullfield
        int pageOffset = 0;
        int ibuffer = 0;

        //only need these if we are reading from a rid pointer in a page
        int newPageNum = 0;
        int newSlotNum = 0;

        //check if rid exists within current page
        memcpy(&pageOffset, (char*)pageData +(4088), sizeof(int));
        if(pageOffset >= rid.slotNum)
            return -1;

        //get nullfield element to check if we have pointer of another rid
        memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));
        memcpy(&newPageNum, (char*)pageData +(rid.slotNum-sizeof(int)*2),sizeof(int));
        if (newPageNum == 0)
        {
            memcpy(&newPageNum, (char*)pageData + pageOffset,sizeof(int));
            memcpy(&newSlotNum, (char*)pageData + pageOffset + sizeof(int),sizeof(int));

            if(fileHandle.readPage(newPageNum, pageData) == -1)
                return -1;

            memcpy(&pageOffset, (char*)pageData + newSlotNum,sizeof(int));
        }

        if(pageOffset == -1)
            return -1;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char nullbits[nullField];
//        nullbits = (unsigned char*)malloc(nullField);
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

        free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {

        void * pageData;
        pageData = malloc(4096);

        if(fileHandle.readPage(rid.pageNum, pageData) == -1)
            return -1;

        int recordOffsetField = 4092;
        int RIDOffsetFields = 4088;
        int totalRecordOffsets = 0;
        int totalRIDOffsets = 0;

        memcpy(&totalRecordOffsets, (char *) pageData + recordOffsetField, sizeof(int));
        memcpy(&totalRIDOffsets, (char *) pageData + RIDOffsetFields, sizeof(int));

        int nullfieldSize = 0;
        int slotBegin = 0;
        int slotEnd = 0;

        memcpy(&slotBegin, (char *) pageData + (rid.slotNum), sizeof(int));
        memcpy(&slotEnd, (char *) pageData + (rid.slotNum-sizeof(int)), sizeof(int));
        memcpy(&nullfieldSize, (char *) pageData + (rid.slotNum-sizeof(int)*2), sizeof(int));

        //can't delete already deleted record
        if(nullfieldSize == -1 || slotBegin == -1 || slotEnd == -1)
            return -1;

        int holder_value = -1;

        memcpy((char *) pageData + (rid.slotNum), &holder_value, sizeof(int));
        memcpy((char *) pageData + (rid.slotNum-sizeof(int)), &holder_value, sizeof(int));
        memcpy((char *) pageData + (rid.slotNum-sizeof(int)*2), &holder_value, sizeof(int));

        memcpy((char *) pageData + slotBegin, (char *) pageData + slotEnd, (totalRecordOffsets-slotEnd));

        int index = 0;
        for(int i = 4084; i > totalRIDOffsets; i -= sizeof(int))
        {
            memcpy(&holder_value, (char *) pageData + i, sizeof(int));

            //we want to ignore adjusting the nullsize fields also since we will have random rid slot values we need
            // to make sure we only move the ones > or after the current record entry
            if (holder_value != -1 && index%3 != 2 && holder_value >= slotEnd)
            {
                holder_value -= (slotEnd-slotBegin);
                memcpy((char *) pageData + i, &holder_value, sizeof(int));
            }

            ++index;
        }

        totalRecordOffsets -= (slotEnd-slotBegin);
        memcpy((char *) pageData + recordOffsetField, &totalRecordOffsets, sizeof(int));

        fileHandle.writePage(rid.pageNum, pageData);
        free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        int offset = 0;// nullField; //for nullfield
        int ibuffer = 0;
        float fbuffer = 0.0;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

//        int v = 0;
//        for(int j =0; j<25;j++)
//        {
//            memcpy(&v, (char *) data +j, sizeof(int));
//            std::cout<< j<< ":" << v << "\n";
//        }

//        std::cout<< nullField<<"\n";
        unsigned char nullbits[nullField];
//        nullbits = (unsigned char*)malloc(nullField);
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
                        out << recordDescriptor[i].name << ":" << useStr << ", ";//useStr << ", ";
                        break;
                }
            }
            out << "\n";
        }

        return 0;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {

        void * pageData;
        pageData = malloc(4096);

        if(deleteRecord(fileHandle,recordDescriptor,rid) == -1)
            return -1;

        if(fileHandle.readPage(rid.pageNum, pageData) == -1)
            return -1;

        int recordOffsetField = 4092;
        int RIDOffsetFields = 4088;
        int totalRecordOffsets = 0;
        int totalRIDOffsets = 0;

        memcpy(&totalRecordOffsets, (char *) pageData + recordOffsetField, sizeof(int));
        memcpy(&totalRIDOffsets, (char *) pageData + RIDOffsetFields, sizeof(int));

        int slotOffset = 0;
        int ibuffer = 0;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char nullbits[nullField];
//        nullbits = (unsigned char*)malloc(nullField);
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

        if((totalRIDOffsets-totalRecordOffsets) > (slotOffset + (sizeof(int)*4)))
        {
            //add new data
            memcpy((char *) pageData + totalRecordOffsets, (char *) data, slotOffset);

            //add new record feild in page
            memcpy((char *) pageData + (rid.slotNum), &totalRecordOffsets, sizeof(int));
            totalRecordOffsets += slotOffset;
            memcpy((char *) pageData + (rid.slotNum-sizeof(int)), &totalRecordOffsets, sizeof(int));
            memcpy((char *) pageData + (rid.slotNum-sizeof(int)*2), &nullField, sizeof(int));

            //update offset values at end of page
            memcpy((char *) pageData + recordOffsetField, &totalRecordOffsets, sizeof(unsigned));

            ibuffer = fileHandle.writePage(rid.pageNum, pageData);
            free(pageData);
            if (ibuffer == -1)
                return -1;
            else
                return 0;
        }
        else
        {
            void * pageData2;
            pageData2 = malloc(4096);

            int new_pageNum = 0;
            int new_slotNum = 4084;

            unsigned readCount = 0;
            unsigned writeCount = 0;
            unsigned appendCount = 0;

            fileHandle.collectCounterValues(readCount,writeCount,appendCount);

            int totalRecordOffsets2 = 0;
            int totalRIDOffsets2 = 0;
            for(int i=-1; i+1 < appendCount+1; i++)
            {
                if(i==-1)
                    fileHandle.readPage(appendCount-1, pageData2);
                else
                    fileHandle.readPage(i, pageData2);

                memcpy(&totalRecordOffsets2, (char *) pageData2 + recordOffsetField, sizeof(int));
                memcpy(&totalRIDOffsets2, (char *) pageData2 + RIDOffsetFields, sizeof(int));

                if((totalRIDOffsets2-totalRecordOffsets2) > (slotOffset + (sizeof(int)*4)))
                {
                    //set rids
                    if(i==-1)
                        new_pageNum = appendCount-1;
                    else
                        new_pageNum = i;

                    //search for empty slots to reuse
                    bool found_empty = false;
                    int RID_offset = totalRIDOffsets2 + sizeof(int)*2;
                    new_slotNum = totalRIDOffsets2 - sizeof(int);
                    for(int j = RID_offset; j < RIDOffsetFields ; j += sizeof(int)*3)
                    {
                        memcpy(&RID_offset, (char *) pageData2 + j, sizeof(int));

                        if(RID_offset == -1) {
                            new_slotNum = j;
                            found_empty = true;
                        }
                    }

                    //add new data
                    memcpy((char *) pageData2 + totalRecordOffsets2, (char *) data, slotOffset);

                    //add new record feild in page
                    memcpy((char *) pageData2 + (new_slotNum), &totalRecordOffsets2, sizeof(int));
                    totalRecordOffsets2 += slotOffset;
                    memcpy((char *) pageData2 + (new_slotNum-sizeof(int)), &totalRecordOffsets2, sizeof(int));
                    memcpy((char *) pageData2 + (new_slotNum-sizeof(int)*2), &nullField, sizeof(int));

                    //update offset values at end of page
                    memcpy((char *) pageData2 + recordOffsetField, &totalRecordOffsets2, sizeof(unsigned));

                    //if we need to add a new slot record
                    if(!found_empty)
                    {
                        totalRIDOffsets2 -= 3*sizeof(int);
                        memcpy((char *) pageData2 + RIDOffsetFields, &totalRIDOffsets2, sizeof(unsigned ));
                    }

                    if(i==-1)
                        ibuffer = fileHandle.writePage(appendCount-1, pageData2);
                    else
                        ibuffer = fileHandle.writePage(i, pageData2);
                    if (ibuffer == -1)
                        return -1;

                    //update old rid slot with pointer
                    int new_ptr_size = totalRecordOffsets + sizeof(int)*2;
                    int empty_nullfield = 0;
                    memcpy((char *) pageData + totalRecordOffsets, &new_pageNum, sizeof(int));
                    memcpy((char *) pageData + totalRecordOffsets + sizeof(int), &new_slotNum, sizeof(int));

                    memcpy((char *) pageData + rid.slotNum, &totalRecordOffsets, sizeof(int));
                    memcpy((char *) pageData + rid.slotNum-sizeof(int), &new_ptr_size, sizeof(int));
                    memcpy((char *) pageData + rid.slotNum-sizeof(int)*2, &empty_nullfield, sizeof(int));
                    //update page with the 8 new bytes for the pointer values
                    memcpy((char *) pageData + recordOffsetField, &new_ptr_size, sizeof(int));

                    ibuffer = fileHandle.writePage(rid.pageNum, pageData);
                    free(pageData2);
                    free(pageData);
                    if (ibuffer == -1)
                        return -1;
                    else
                        return 0;
                }
            }

            //if here no page is large enough overwrite the copy of the previous page
            memcpy((char *) pageData2, (char *) data, slotOffset);

            //slotOffset is length of the record so:
            int slotbegin=0;
            int RIDOffsetvalue2 = 4076;
            memcpy((char *) pageData2 + (RIDOffsetFields-sizeof(int)), &slotbegin, sizeof(int));
            memcpy((char *) pageData2 + (RIDOffsetFields-sizeof(int)*2), &slotOffset, sizeof(int));
            memcpy((char *) pageData2 + (RIDOffsetFields-sizeof(int)*3), &nullField, sizeof(int));
            memcpy((char *) pageData2 + RIDOffsetFields, &RIDOffsetvalue2, sizeof(int ));
            memcpy((char *) pageData2 + recordOffsetField, &slotOffset, sizeof(int));

            ibuffer = fileHandle.appendPage(pageData2);
            if (ibuffer == -1)
                return -1;

            fileHandle.collectCounterValues(readCount,writeCount,appendCount);

            //update old rid slot with pointer
            new_pageNum = appendCount-1;
            new_slotNum = RIDOffsetFields-sizeof(int);
            int new_ptr_size = totalRecordOffsets + sizeof(int)*2;
            int empty_nullfield = 0;
            memcpy((char *) pageData + totalRecordOffsets, &new_pageNum, sizeof(int));
            memcpy((char *) pageData + totalRecordOffsets + sizeof(int), &new_slotNum, sizeof(int));

            memcpy((char *) pageData + rid.slotNum, &totalRecordOffsets, sizeof(int));
            memcpy((char *) pageData + rid.slotNum-sizeof(int), &new_ptr_size, sizeof(int));
            memcpy((char *) pageData + rid.slotNum-sizeof(int)*2, &empty_nullfield, sizeof(int));
            //update page with the 8 new bytes for the pointer values
            memcpy((char *) pageData + recordOffsetField, &new_ptr_size, sizeof(int));

            ibuffer = fileHandle.writePage(rid.pageNum, pageData);
            free(pageData2);
            free(pageData);
            if (ibuffer == -1)
                return -1;
            else
                return 0;
        }


        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        void * pageData;
        pageData = malloc(4096);

        if(fileHandle.readPage(rid.pageNum, pageData) == -1)
            return -1;

        int pageOffset = 0;
        int ibuffer = 0;

        //only need these if we are reading from a rid pointer in a page
        int newPageNum = 0;
        int newSlotNum = 0;

        //get nullfield element to check if we have pointer of another rid
        memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));
        memcpy(&newPageNum, (char*)pageData +(rid.slotNum-sizeof(int)*2),sizeof(int));
        if (newPageNum == 0)
        {
            memcpy(&newPageNum, (char*)pageData + pageOffset,sizeof(int));
            memcpy(&newSlotNum, (char*)pageData + pageOffset + sizeof(int),sizeof(int));

            if(fileHandle.readPage(newPageNum, pageData) == -1)
                return -1;

            memcpy(&pageOffset, (char*)pageData + newSlotNum,sizeof(int));
        }

        if(pageOffset == -1)
            return -1;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char nullbits[nullField];
//        nullbits = (unsigned char*)malloc(nullField);
        memcpy(nullbits, (char*)pageData + pageOffset, nullField);
        pageOffset += nullField;

        unsigned char nbit = (unsigned char) 0 << (unsigned) 7;
        for(int i=0; i < recordDescriptor.size();i++)
        {
            bool nullBit = nullbits[int(floor((double) i/8))] & ((unsigned) 1 << (unsigned) (7-(i%8)));
            if(!nullBit) {
                switch (recordDescriptor[i].type) {
                    case 0:
                        if(recordDescriptor[i].name == attributeName)
                            memcpy((char *) data+1, (char *) pageData + pageOffset, sizeof(int));
                        pageOffset += sizeof(int);
                        break;
                    case 1:
                        if(recordDescriptor[i].name == attributeName)
                            memcpy((char *) data+1, (char *) pageData + pageOffset, sizeof(float));
                        pageOffset += sizeof(float);
                        break;
                    case 2:
                        ibuffer = 0;
                        memcpy(&ibuffer, (char *) pageData + pageOffset, sizeof(int));
                        if(recordDescriptor[i].name == attributeName)
                            memcpy((char *) data+1, &ibuffer, sizeof(int));
                        pageOffset += sizeof(int);

                        if(recordDescriptor[i].name == attributeName)
                            memcpy((char *) data+1 + sizeof(int), (char *) pageData + pageOffset, ibuffer);
                        pageOffset += ibuffer;
                        break;
                }
            }
            else
            {
                if(recordDescriptor[i].name == attributeName)
                    nbit += (unsigned char) 1 << (unsigned) 7;
            }
        }

        memcpy((char *) data, &nbit, 1);
        free(pageData);
        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        void* data;
        data = malloc(4096);

        void* pageData;
        pageData = malloc(4096);

        unsigned char nullTestVar = 0;

        unsigned end_of_page = 0;
        RID rid;
        rid.pageNum = 0;
        rid.slotNum = 4084;

        //get first page
        if(fileHandle.readPage(rid.pageNum, pageData) == -1)
            return -1;

        memcpy(&end_of_page, (char*)pageData+4088, sizeof(unsigned));

        //=====================CHECKING FOR UPDATED VALUES========================
        //only need these if we are reading from a rid pointer in a page
        int pageOffset = 0;
        int newPageNum = 0;
        int newSlotNum = 0;
        bool updated_value = false;
        RID rid_swap;
        //===============================END CHECK================================

        bool accept_data = true;
        int int_holder = 0;
        int compInt;
        float float_holder = 0.0;
        float compFloat;
        const char* str_holder;
        void* str_holder_data = malloc(500);
        std::string compStr;
        int dataType = 0; //int 0, float 1, string 2
        if (conditionAttribute != "")
        {
            accept_data = false;
            for (int i = 0; i < recordDescriptor.size(); i++)
            {
                if (recordDescriptor[i].name == conditionAttribute)
                {
                    if (recordDescriptor[i].type == AttrType::TypeVarChar)
                    {
                        dataType = 2;
                        compStr = static_cast<const char*>(value);
//                        compInt = *reinterpret_cast<const int*>(value);
                    } else if (recordDescriptor[i].type == AttrType::TypeReal)
                    {
                        dataType = 1;
                        compFloat = *reinterpret_cast<const float*>(value);
                    }
                    else
                        compInt = *reinterpret_cast<const int*>(value);

                    i = recordDescriptor.size();
                }
            }
        }

//        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
//        rbfm_ScanIterator.attributeNames = attributeNames;

        while(rid.pageNum < fileHandle.appendPageCounter)
        {
            while(end_of_page >= rid.slotNum && rid.pageNum < fileHandle.appendPageCounter)
            {
                rid.pageNum += 1;
                rid.slotNum = 4084;
                if(rid.pageNum < fileHandle.appendPageCounter)
                {
                    fileHandle.readPage(rid.pageNum, pageData);
                    end_of_page = 0;
                    memcpy(&end_of_page, (char*)pageData+4088, sizeof(unsigned));
                }
            }

            if(rid.pageNum < fileHandle.appendPageCounter)
            {
                //=====================CHECKING FOR UPDATED VALUES========================
                //check if rid exists within current page
                memcpy(&pageOffset, (char*)pageData +(4088), sizeof(int));
                if(pageOffset >= rid.slotNum)
                    return -1;

                //get nullfield element to check if we have pointer of another rid
                memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));
                memcpy(&newPageNum, (char*)pageData +(rid.slotNum-sizeof(int)*2),sizeof(int));
                if (newPageNum == 0)
                {
                    memcpy(&newPageNum, (char*)pageData + pageOffset,sizeof(int));
                    memcpy(&newSlotNum, (char*)pageData + pageOffset + sizeof(int),sizeof(int));

                    rid_swap.pageNum = rid.pageNum;
                    rid_swap.slotNum = rid.slotNum;

                    rid.pageNum = newPageNum;
                    rid.slotNum = newSlotNum;

                    if(fileHandle.readPage(newPageNum, pageData) == -1)
                        return -1;

                    memcpy(&pageOffset, (char*)pageData + newSlotNum,sizeof(int));

                    updated_value = true;
                }

                if(pageOffset == -1)
                    return -1;
                //===============================END CHECK================================

                //check if delete slot id
                memcpy(&int_holder, (char*) pageData + rid.slotNum, sizeof(int));

                if(int_holder != -1)
                {
                    if(conditionAttribute != "")
                    {
                        readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, data);
                        memcpy(&nullTestVar, (char *)data, 1);
                        accept_data = false;
                        if(!bool((nullTestVar & ((unsigned) 1 << (unsigned) 7))))
                        {
                            // we dont know what values we will need to check so we will need nested switch statments
                            switch(dataType)
                            {
                                case 0:
                                    memcpy(&int_holder, (char*) data+1, sizeof(int));
                                    switch(compOp)
                                    {
                                        case 0:
                                            accept_data = (int_holder == compInt);
                                            break;
                                        case 1:
                                            accept_data = (int_holder < compInt);
                                            break;
                                        case 2:
                                            accept_data = (int_holder <= compInt);
                                            break;
                                        case 3:
                                            accept_data = (int_holder > compInt);
                                            break;
                                        case 4:
                                            accept_data = (int_holder >= compInt);
                                            break;
                                        case 5:
                                            accept_data = (int_holder != compInt);
                                            break;
                                        case 6:
                                            accept_data = true;
                                            break;
                                    }
                                    break;
                                case 1:
                                    memcpy(&float_holder, (char*) data+1, sizeof(float));
                                    switch(compOp)
                                    {
                                        case 0:
                                            accept_data = (float_holder == compFloat);
                                            break;
                                        case 1:
                                            accept_data = (float_holder < compFloat);
                                            break;
                                        case 2:
                                            accept_data = (float_holder <= compFloat);
                                            break;
                                        case 3:
                                            accept_data = (float_holder > compFloat);
                                            break;
                                        case 4:
                                            accept_data = (float_holder >= compFloat);
                                            break;
                                        case 5:
                                            accept_data = (float_holder != compFloat);
                                            break;
                                        case 6:
                                            accept_data = true;
                                            break;
                                    }
                                    break;
                                case 2:
                                    memcpy(&int_holder, (char*) data+1, sizeof(int));
                                    memcpy((char*)str_holder_data, (char*) data+5, int_holder);
                                    str_holder = reinterpret_cast<const char*>(str_holder_data);
                                    switch(compOp)
                                    {

                                        case 0:
                                            accept_data = (str_holder == compStr);
                                            break;
                                        case 1:
                                            accept_data = (str_holder < compStr);
                                            break;
                                        case 2:
                                            accept_data = (str_holder <= compStr);
                                            break;
                                        case 3:
                                            accept_data = (str_holder > compStr);
                                            break;
                                        case 4:
                                            accept_data = (str_holder >= compStr);
                                            break;
                                        case 5:
                                            accept_data = (str_holder != compStr);
                                            break;
                                        case 6:
                                            accept_data = true;
                                            break;
                                    }
                                    break;
                            }
                        }
                    }

                    if(accept_data)
                        rbfm_ScanIterator.rids.push_back(rid);
                }

                //if we found an updated value we need to reset rid values and reload page
                if(updated_value)
                {
                    rid.pageNum = rid_swap.pageNum;
                    rid.slotNum = rid_swap.slotNum;
                    fileHandle.readPage(rid.pageNum, pageData);
                    updated_value = false;
                }

                rid.slotNum -= 12;
            }
        }

        free(data);
        free(pageData);
        free(str_holder_data);
        return 0;
    }

    RC RecordBasedFileManager::custom_extract(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                              std::vector<std::string> attributeNames,  int &current_page,
                                              RID &rid, void *pageData, void *data)
    {
        if(current_page != rid.pageNum)
        {
            if(fileHandle.readPage(rid.pageNum, pageData) == -1)
                return -1;
            current_page = rid.pageNum;
        }

//        void* test_str = malloc(6);
//        const char* thing;
//        memcpy((char*)test_str, (char*)pageData + 9,6);
//        thing = reinterpret_cast<const char*>(test_str);

        int dataOffset = 0;
        int pageOffset = 0;
        int ibuffer = 0;

        //only need these if we are reading from a rid pointer in a page
        int newPageNum = 0;
        int newSlotNum = 0;

        //get nullfield element to check if we have pointer of another rid
        memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));
        memcpy(&newPageNum, (char*)pageData +(rid.slotNum-sizeof(int)*2),sizeof(int));
        if (newPageNum == 0)
        {
            memcpy(&newPageNum, (char*)pageData + pageOffset,sizeof(int));
            memcpy(&newSlotNum, (char*)pageData + pageOffset + sizeof(int),sizeof(int));

            if(fileHandle.readPage(newPageNum, pageData) == -1)
                return -1;

            memcpy(&pageOffset, (char*)pageData + newSlotNum,sizeof(int));
        }

        if(pageOffset == -1)
            return -1;

        int fieldCount_R = attributeNames.size();
        int nullField_R = ceil((double) fieldCount_R/ 8);
        unsigned char nullbits_R[nullField_R];
        //set the datas nullfeilds
        for(int i = 0; i < nullField_R; i++)
            nullbits_R[i] = 0;
        dataOffset += nullField_R;

        int fieldCount = recordDescriptor.size();
        int nullField = ceil((double) fieldCount/ 8);

        unsigned char nullbits[nullField];
        memcpy(nullbits, (char*)pageData + pageOffset, nullField);
        pageOffset += nullField;

        //currently in the scan attribute names are not in order with the attributes vector
        //loop the whole record for each attribute to adhere to the insertRecord schema
        int prePageOffset = pageOffset;
        for(int rs = 0; rs < attributeNames.size(); rs++)
        {
            pageOffset = prePageOffset;
            for (int i = 0; i < recordDescriptor.size(); i++)
            {
                bool nullBit = nullbits[int(floor((double) i / 8))] & ((unsigned) 1 << (unsigned) (7 - (i % 8)));
                if (!nullBit) {
                    switch (recordDescriptor[i].type) {
                        case 0:
                            if (recordDescriptor[i].name == attributeNames[rs])
                            {
                                memcpy((char *) data + dataOffset, (char *) pageData + pageOffset, sizeof(int));
                                dataOffset += sizeof(int);
                            }
                            pageOffset += sizeof(int);
                            break;
                        case 1:
                            if (recordDescriptor[i].name == attributeNames[rs])
                            {
                                memcpy((char *) data + dataOffset, (char *) pageData + pageOffset, sizeof(float));
                                dataOffset += sizeof(float);
                            }
                            pageOffset += sizeof(float);
                            break;
                        case 2:
                            ibuffer = 0;
                            memcpy(&ibuffer, (char *) pageData + pageOffset, sizeof(int));
                            if (recordDescriptor[i].name == attributeNames[rs])
                            {
                                memcpy((char *) data + dataOffset, &ibuffer, sizeof(int));
                                dataOffset += sizeof(int);
                                memcpy((char *) data + dataOffset, (char *) pageData + pageOffset + sizeof(int), ibuffer);
                                dataOffset += ibuffer;
                            }
                            pageOffset += sizeof(int);
                            pageOffset += ibuffer;
                            break;
                    }
                } else {
                    if (recordDescriptor[i].name == attributeNames[rs])
                        nullbits_R[int(floor((double) rs / 8))] += ((unsigned) 1 << (unsigned) (7 - (rs % 8)));
                }
            }
        }
        memcpy((char *) data, &nullbits_R, nullField_R);
        return 0;
    }

} // namespace PeterDB

