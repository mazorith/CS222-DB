#ifndef _rbfm_h_
#define _rbfm_h_

#include <vector>
#include <cmath>

#include "pfm.h"

namespace PeterDB {
    // Record ID
    typedef struct {
        unsigned pageNum = 0;           // page number
        unsigned short slotNum = 0;     // slot number in the page
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0, // no condition// =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator() = default;;

        ~RBFM_ScanIterator() = default;;

        std::vector<RID> rids;
//        void* pageData = malloc(4096);
        unsigned current_page = 0;
        unsigned index = 0;
        bool first_fix = true;

        std::string filename = "";
//        PagedFileManager * pfm;
//        FileHandle fileHandle;

//        std::vector<Attribute> recordDescriptor;
//        std::vector<std::string> attributeNames;

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data)
        {
            if(first_fix)
            {
                first_fix = false;
                current_page = 0;
                index = 0;
//                pfm = &PagedFileManager::instance();
//                pfm->openFile(filename,fileHandle);
            }

            if(index >= rids.size())
            {
//                free(pageData);
//                pfm->closeFile(fileHandle);
                return RBFM_EOF;
            }
            else
            {
//                if(current_page != rids[index].pageNum)
//                {
//                    if(fileHandle.readPage(rids[index].pageNum, pageData) == -1)
//                        return -1;
//                    current_page = rids[index].pageNum;
//                }

                rid.pageNum = rids[index].pageNum;
                rid.slotNum = rids[index].slotNum;

                int dataOffset = 0;
                int pageOffset = 0;
                int ibuffer = 0;

                //only need these if we are reading from a rid pointer in a page
                int newPageNum = 0;
                int newSlotNum = 0;

//                //get nullfield element to check if we have pointer of another rid
//                memcpy(&pageOffset, (char*)pageData +(rid.slotNum),sizeof(int));
//                memcpy(&newPageNum, (char*)pageData +(rid.slotNum-sizeof(int)*2),sizeof(int));
//                if (newPageNum == 0)
//                {
//                    memcpy(&newPageNum, (char*)pageData + pageOffset,sizeof(int));
//                    memcpy(&newSlotNum, (char*)pageData + pageOffset + sizeof(int),sizeof(int));
//
//                    if(fileHandle.readPage(newPageNum, pageData) == -1)
//                        return -1;
//
//                    memcpy(&pageOffset, (char*)pageData + newSlotNum,sizeof(int));
//                }
//
//                if(pageOffset == -1)
//                    return -1;
//
//                int fieldCount_R = attributeNames.size();
//                int nullField_R = ceil((double) fieldCount_R/ 8);
//                unsigned char nullbits_R[nullField_R];
//                //set the datas nullfeilds
//                for(int i = 0; i < nullField_R; i++)
//                    nullbits_R[i] = 0;
//                dataOffset += nullField_R;
//
//                int fieldCount = recordDescriptor.size();
//                int nullField = ceil((double) fieldCount/ 8);
//
//                unsigned char nullbits[nullField];
//                memcpy(nullbits, (char*)pageData + pageOffset, nullField);
//                pageOffset += nullField;
//
//                int attr_index = 0; //I am assuming the attribute names are in order with the record descriptor
//                for(int i=0; i < recordDescriptor.size();i++)
//                {
//
//                    bool nullBit = nullbits[int(floor((double) i/8))] & ((unsigned) 1 << (unsigned) (7-(i%8)));
//                    if(!nullBit) {
//                        switch (recordDescriptor[i].type) {
//                            case 0:
//                                if(recordDescriptor[i].name == attributeNames[attr_index])
//                                {
//                                    memcpy((char *) data+dataOffset, (char *) pageData + pageOffset, sizeof(int));
//                                    attr_index++;
//                                    dataOffset += sizeof(int);
//                                }
//                                pageOffset += sizeof(int);
//                                break;
//                            case 1:
//                                if(recordDescriptor[i].name == attributeNames[attr_index])
//                                {
//                                    memcpy((char *) data + dataOffset, (char *) pageData + pageOffset, sizeof(float));
//                                    attr_index++;
//                                    dataOffset += sizeof(float);
//                                }
//                                pageOffset += sizeof(float);
//                                break;
//                            case 2:
//                                ibuffer = 0;
//                                memcpy(&ibuffer, (char *) pageData + pageOffset, sizeof(int));
//                                if(recordDescriptor[i].name == attributeNames[attr_index])
//                                {
//                                    memcpy((char *) data+dataOffset, &ibuffer, sizeof(int));
//                                    dataOffset += sizeof(int);
//                                }
//                                pageOffset += sizeof(int);
//
//                                if(recordDescriptor[i].name == attributeNames[attr_index])
//                                {
//                                    memcpy((char *) data+dataOffset + sizeof(int), (char *) pageData + pageOffset, ibuffer);
//                                    attr_index++;
//                                    dataOffset += ibuffer;
//                                }
//                                pageOffset += ibuffer;
//                                break;
//                        }
//                    }
//                    else
//                    {
//                        if(recordDescriptor[i].name == attributeNames[attr_index])
//                        {
//                            nullbits_R[int(floor((double) attr_index/8))] += ((unsigned) 1 << (unsigned) (7-(attr_index%8)));
//                            attr_index++;
//                        }
//                    }
//                }
//                memcpy((char *) data, &nullbits_R, nullField_R);
                return 0;
            }

        };

        RC close()
        {
            return -1;
        };
    };

    class RecordBasedFileManager {
    public:
        void * page;

        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        //due to some currently unexplainable errors I need this custsom_extract function
        RC custom_extract(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                          std::vector<std::string> attributeNames, int &current_page,
                          RID &rid, void *pageData, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    };

} // namespace PeterDB

#endif // _rbfm_h_
