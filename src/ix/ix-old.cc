#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if(std::fopen(fileName.c_str(), "r") != NULL)
            return -1;
        else
        {
            std::ofstream newFile;
            newFile.open(fileName);
            newFile.close();
        }

        return 0;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if(std::fopen(fileName.c_str(), "r") != NULL)
            std::remove(fileName.c_str());
        else
            return -1;

        return 0;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if(FILE * info = std::fopen(fileName.c_str(), "r"))
        {
            if(fileName.c_str() == ixFileHandle.handle.file)
                return -1;

            ixFileHandle.setFileName(fileName);

            fseek(info, 0, SEEK_END);
            if (ftell(info) >= (1) *4096)
            {
                fseek(info, -4096, SEEK_END);
                unsigned value;

                //TODO:Read root data from page
//                fread((char*)&value, sizeof(unsigned), 1, info);
//                ixFileHandle.totalPages = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                ixFileHandle.ixAppendPageCounter = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                ixFileHandle.ixWritePageCounter = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                ixFileHandle.ixReadPageCounter = value;

                ixFileHandle.has_saved_values = true;
            }

            fclose(info);
        }
        else
            return -1;

        return 0;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {

        //TODO: save root data to page
        if (ixFileHandle.has_saved_values == true)
        {
            FILE * info = std::fopen(ixFileHandle.handle.file, "r+");

            fseek(info, -4096, SEEK_END);

//            fwrite((char*)&ixFileHandle.totalPages, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixAppendPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixWritePageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixReadPageCounter, sizeof(unsigned), 1, info);

            fclose(info);
        }
        else
        {
            FILE * info = std::fopen(ixFileHandle.handle.file, "a");

//            fwrite((char*)&ixFileHandle.totalPages, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixAppendPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixWritePageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&ixFileHandle.ixReadPageCounter, sizeof(unsigned), 1, info);

            for(int i = 0; i < 4084; i++) //1023 = 4096 (use 4080 if using totalPages)
                fwrite((char*)&ixFileHandle.ixAppendPageCounter, sizeof(char), 1, info);

            fclose(info);
        }

        return 0;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        //Format of the B+ tree will be 3*4 bytes for page pointers,

        std::vector<RID> path;

        char* str_key;
        unsigned int_key;
        float real_key;

        //holder variables we can efficently reuse
        int int_holder=0;
        float real_holder=0.0;
        char* str_holder;

        //key type
        int selection = 0; //0 for int, 1 for real, 2 for varchar (doing this so I dont have the PeterDB bloat later)
        if(attribute.type == PeterDB::TypeInt)
            int_key = *reinterpret_cast<const int *>(key);
        else if(attribute.type == PeterDB::TypeReal) {
            real_key = *reinterpret_cast<const float *>(key);
            selection = 1;
        }
        //this wont work for strings but it is ok for now
        else if(attribute.type == PeterDB::TypeVarChar)
        {
            memcpy(&int_key, (char*) key, sizeof(unsigned));
            str_key = (char*)key+4;
            selection = 2;
        }

        void* pageData = malloc(4096);

        //will be coding for just double int/floats for the time being
        if(ixFileHandle.ixAppendPageCounter == 0)
        {
            root.pageNum = -1;
            root.slotNum = -1;

            //create record for leaf node (for each leaf we reserve enough space for at least one rid of each key
            int begin_offset = 0;
            int end_offset = 64;
            memcpy((char*)pageData+4080, &begin_offset, sizeof(int));
            memcpy((char*)pageData+4084, &end_offset, sizeof(int));

            //Add leafnode and its data
            int is_leaf = 0;
            int entry1_offset = 32;
            int entry2_offset = -1;
            int entry3_offset = -1;
            int entry4_offset = -1;
            int end_of_entry4 = -1;

            RID nextPageID;
            nextPageID.pageNum = -1;
            nextPageID.slotNum = -1;

            memcpy((char*)pageData, &is_leaf, sizeof(int));
            memcpy((char*)pageData+4, &entry1_offset, sizeof(int));
            memcpy((char*)pageData+8, &entry2_offset, sizeof(int));
            memcpy((char*)pageData+12, &entry3_offset, sizeof(int));
            memcpy((char*)pageData+16, &entry4_offset, sizeof(int));
            memcpy((char*)pageData+20, &end_of_entry4, sizeof(int));
            memcpy((char*)pageData+24, &nextPageID.pageNum, sizeof(int));
            memcpy((char*)pageData+28, &nextPageID.slotNum , sizeof(int));

            int TotalOffset = 64;
            int ridOffsets = 4080;
            switch(selection)
            {
                case 0:
                    memcpy((char*)pageData+32, (char*)key, sizeof(int));
                    memcpy((char*)pageData+36, &rid.pageNum, sizeof(int));
                    memcpy((char*)pageData+40, &rid.slotNum, sizeof(int));

                    entry2_offset = 44;
                    memcpy((char*)pageData+8, &entry2_offset, sizeof(int));
                    break;
                case 1:
                    memcpy((char*)pageData+32, (char*)key, sizeof(float));
                    memcpy((char*)pageData+36, &rid.pageNum, sizeof(int));
                    memcpy((char*)pageData+40, &rid.slotNum, sizeof(int));

                    entry2_offset = 44;
                    memcpy((char*)pageData+8, &entry2_offset, sizeof(int));
                    break;
                case 2:
                    memcpy((char*)pageData+32, &int_key, sizeof(float));
                    memcpy((char*)pageData+36, str_key, int_key);
                    memcpy((char*)pageData+36+int_key, &rid.pageNum, sizeof(int));
                    memcpy((char*)pageData+40+int_key, &rid.slotNum, sizeof(int));

                    entry2_offset = 44+int_key;
                    memcpy((char*)pageData+8, &entry2_offset, sizeof(int));

                    TotalOffset = entry2_offset;
                    break;
            }


            //update total and RID page offsets
            //int and floats will follow a standard format while varchar will not ;_;
            memcpy((char*)pageData+4092, &TotalOffset, sizeof(int));
            memcpy((char*)pageData+4088, &ridOffsets, sizeof(int));

            int return_val = ixFileHandle.appendPage(pageData);
            free(pageData);
            return return_val;
        }
        else
        {
            //first check if we have a root otherwise we just want to insert a new index into the "page"
            if(root.pageNum == -1)
            {
                if(ixFileHandle.readPage(0,pageData) == -1)
                    return -1;

                int offsets[5];
                int filled_values = 0;
                int add_index = 0;
                //check all keys for match first:
                for(int i = 0; i < 4; i++)
                {
                    memcpy(&offsets[i], (char*)pageData+((i*4) + 4), sizeof(int));
                    memcpy(&offsets[i+1], (char*)pageData+(((i+1)*4) + 4), sizeof(int));
                    if (offsets[i+1] != -1)
                        filled_values += 1;
                    else if(add_index == 0)
                        add_index = i;
                }


                //first check if a our insert key is lesser than any existing keys (these should already be in sorted order)
                int index = 0;
                bool found_earlier_slot = false;
                for(; index < 4; index++)
                {
                    //check if we have another value
                    if(offsets[index+1] == -1)
                        break;

                    switch(selection)
                    {
                        case 0:
                            memcpy(&int_holder, (char*)pageData+offsets[index], sizeof(int));
                            found_earlier_slot = int_key <= int_holder;
                            break;
                        case 1:
                            memcpy(&real_holder, (char*)pageData+offsets[index], sizeof(float));
                            found_earlier_slot = real_key <= real_holder;
                            break;
                        case 2:
                            memcpy(&int_holder, (char*)pageData+offsets[index], sizeof(int));
                            memcpy(str_holder, (char*)pageData+offsets[index]+4, int_holder);
                            break;
                    }

                    //if the keys are the same also break before we update index
                    if(found_earlier_slot)
                        break;
                }

                //if we found an earlier slot to insert into, we need to shift everything and update the values
                if(found_earlier_slot)
                {
                    //do the shift
                    memcpy(&int_holder, (char*)pageData+4084, sizeof(int));
                    if(selection == 0 || selection == 1)
                        memcpy((char*)pageData+(offsets[index]+12), (char*)pageData+(offsets[index]), (int_holder-offsets[index]));
                    else
                        memcpy((char*)pageData+(offsets[index]+12+int_key), (char*)pageData+(offsets[index]), (int_holder-offsets[index]));

                    //add new values
                    if(selection == 0 || selection == 1)
                    {
                        memcpy((char *) pageData + offsets[index], (char *) key, sizeof(int));
                        memcpy((char *) pageData + offsets[index] + 4, &rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + offsets[index] + 8, &rid.slotNum, sizeof(int));
                    }
                    else
                    {
                        memcpy((char*)pageData+offsets[index], &int_key, sizeof(int));
                        memcpy((char*)pageData+offsets[index]+4, str_key, int_key);
                        memcpy((char*)pageData+offsets[index]+4+int_key, &rid.pageNum, sizeof(int));
                        memcpy((char*)pageData+offsets[index]+8+int_key, &rid.slotNum, sizeof(int));
                    }


                    //update the offsets of the "page"
                    for(int update_index=index; update_index<4; update_index++)
                    {
                        if(offsets[update_index] != -1)
                        {
                            if(selection == 0 || selection == 1)
                                int_holder = offsets[update_index] + 12;
                            else
                                int_holder = offsets[update_index] + 12 + int_key;
                            memcpy((char*)pageData+(((update_index+1)*4)+4), &int_holder, sizeof(int));
                        }
                    }
                }
                //else the insert is very easy (at least for strings in the first case)
                else
                {
                    //add new values
                    if(selection == 0 || selection == 1)
                    {
                        memcpy((char *) pageData + offsets[add_index], (char *) key, sizeof(int));
                        memcpy((char *) pageData + offsets[add_index] + 4, &rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + offsets[add_index] + 8, &rid.slotNum, sizeof(int));

                        //update the offsets of the "page"
                        int_holder = offsets[add_index + 1] + 12;
                        memcpy((char *) pageData + (((add_index + 1) * 4) + 4), &int_holder, sizeof(int));
                    }
                    else
                    {
                        memcpy((char *) pageData + offsets[add_index], &int_key, sizeof(int));
                        memcpy((char *) pageData + offsets[add_index]+4, &str_key, int_key);
                        memcpy((char *) pageData + offsets[add_index]+4+int_key, &rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + offsets[add_index]+8+int_key, &rid.slotNum, sizeof(int));

                        //update the offsets of the "page"
                        int_holder = offsets[add_index + 1] + 12 + int_key;
                        memcpy((char *) pageData + (((add_index + 1) * 4) + 4), &int_holder, sizeof(int));
                    }
                }


                //if our "page" is full we need to split it.
                bool needs_split = true;
                for(int i; i < 5; i++)
                {
                    memcpy(&int_holder, (char*)pageData+((i*4) + 4), sizeof(int));
                    if(needs_split && int_holder == -1)
                    {
                        needs_split = false;
                        break;
                    }
                }

                //here we will do the actual first split
                if(needs_split)
                {
                    int empty_val_indicator = -1;

                    //this is over kill on the malloc but its nice to have wiggle room
                    void* split1 = malloc(4096);
                    void* split2 = malloc(4096);
                    void* node = malloc(4096);

                    int split1_offset = 0;
                    int split2_offset = 0;
                    int node_offset = 0; //dont need this for right now but when dealing with varchars yes

                    int sudoPageBegin = 0;
                    int sudoPageEnd = 0;

                    memcpy(&sudoPageBegin, (char*)pageData+4080, sizeof(int));
                    memcpy(&sudoPageEnd, (char*)pageData+4084, sizeof(int));

                    //copy first half of "page" into temp memory. (also set last two offsets to -1
                    int first_offset = 0;
                    memcpy(&first_offset, (char*)pageData+sudoPageBegin+12, sizeof(int));
                    memcpy((char*)split1, (char*)pageData+sudoPageBegin, first_offset);
                    memcpy((char*)split1+16, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+20, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+24, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+28, &empty_val_indicator, sizeof(int));
                    if(selection == 0 || selection == 1)
                        split1_offset += 64; //manually setting this for ints/real
                    else
                        split1_offset = first_offset;

                    //now we carefully copy the second half of the "page"
                    int second_offset = 0;
                    int third_offset = 0;
                    memcpy(&first_offset, (char*)pageData+sudoPageBegin+12, sizeof(int));
                    memcpy(&second_offset, (char*)pageData+sudoPageBegin+16, sizeof(int));
                    memcpy(&third_offset, (char*)pageData+sudoPageBegin+20, sizeof(int));

                    int first_entry = 32; //standard first entry offset
                    int second_entry = first_entry + (second_offset-first_offset);
                    int third_entry = second_entry + (third_offset-second_offset);

                    memcpy((char*)split2, (char*)pageData, sizeof(int));
                    memcpy((char*)split2+4, &first_entry, sizeof(int));
                    memcpy((char*)split2+8, &second_entry, sizeof(int));
                    memcpy((char*)split2+12, &third_entry, sizeof(int));
                    memcpy((char*)split2+16, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+20, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+24, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+28, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+32, (char*)pageData+first_offset, (third_offset-first_offset));
                    if(selection == 0 || selection == 1)
                        split2_offset += 64+split1_offset; //manually setting this for ints/real
                    else
                        split2_offset = third_entry + split1_offset;

                    //now we can construct the node: Format:
                    //rid-key-rid-key-rid-key-rid-key-rid
                    //keys will have two int values in the begining:
                    //  4bytes - is_leaf value
                    //  4bytes - number of active keys in node (will be one for this first node)
                    int active_keys = 1;
                    memcpy((char*)node, &empty_val_indicator, sizeof(int));
                    memcpy((char*)node+4, &active_keys, sizeof(int));
                    //now we will leave some space between the keys and set them after we save all data into the
                    //active page together

                    switch(selection)
                    {
                        case 0:
                            memcpy(&int_key, (char*)pageData+first_offset, sizeof(int));
                            memcpy((char*)node+16, &int_key, sizeof(int));
                            node_offset = split2_offset + 28;
                            break;
                        case 1:
                            memcpy(&real_key, (char*)pageData+first_offset, sizeof(float));
                            memcpy((char*)node+16, &real_key, sizeof(float));
                            node_offset = split2_offset + 28;
                            break;
                        case 2:
                            memcpy(&int_key, (char*)pageData+first_offset, sizeof(int));
                            memcpy(str_key, (char*)pageData+first_offset+4, int_key);
                            node_offset = split2_offset + 28 + int_key;
                            break;
                    }


                    //all data has been added so now we need to update our page data
                    //first we will add the lesser split as it will take up less space than the "page" in memory
                    //followed by the other half so we can point the first new "page" to the second
                    //then lastly the node so we can point the node to newly created "pages"
                    int totalPageOffset = split1_offset;
                    int totalRIDOffset = 4080;

                    //don't need an RID for node as it will become the root :D
                    RID split1_rid;
                    RID split2_rid;

                    split1_rid.pageNum = 0;
                    split1_rid.slotNum = 4080;

                    memcpy((char*)pageData+sudoPageBegin, (char*)split1, split1_offset);
                    memcpy((char*)pageData+4084, &split1_offset, sizeof(int));

                    memcpy((char*)pageData+4088, &totalRIDOffset, sizeof(int));
                    memcpy((char*)pageData+4092, &totalPageOffset, sizeof(int));

                    //need to check if we have space for new "second" page
                    if((split2_offset-split1_offset) < totalRIDOffset-totalPageOffset-8)
                    {
                        memcpy((char*)pageData+split1_offset, (char*)split2, split2_offset);
                        memcpy((char*)pageData+4072, &split1_offset, sizeof(int));
                        memcpy((char*)pageData+4076, &split2_offset, sizeof(int));

                        totalRIDOffset -= 8;
                        totalPageOffset = split2_offset;
                        memcpy((char*)pageData+4088, &totalRIDOffset, sizeof(int));
                        memcpy((char*)pageData+4092, &totalPageOffset, sizeof(int));

                        split2_rid.pageNum = 0;
                        split2_rid.slotNum = totalRIDOffset;
                    }
                    else
                    {
                        int newOffset = split2_offset-split1_offset;
                        int beginVal = 0;
                        int newRIDOffset = 4080;
                        memcpy((char*)split2+4080, &beginVal, sizeof(int));
                        memcpy((char*)split2+4084, &newOffset, sizeof(int));
                        memcpy((char*)split2+4088, &newRIDOffset, sizeof(int));
                        memcpy((char*)split2+4092, &newOffset, sizeof(int));

                        if(ixFileHandle.appendPage(split2) == -1)
                            return -1;

                        split2_rid.pageNum = 1;
                        split2_rid.slotNum = newRIDOffset;
                    }


                    if(node_offset-(split2_offset-split1_offset) < totalRIDOffset-totalPageOffset-8)
                    {
                        memcpy((char*)pageData+split2_offset, (char*)node, node_offset);
                        memcpy((char*)pageData+4064, &split2_offset, sizeof(int));
                        memcpy((char*)pageData+4068, &node_offset, sizeof(int));

                        //double check if we actually saved the second split onto the first page
                        if(totalRIDOffset == 4072)
                            totalPageOffset = node_offset;
                        else
                            totalPageOffset = node_offset - split2_offset;

                        totalRIDOffset -= 8;

                        memcpy((char*)pageData+4092, &totalPageOffset, sizeof(int));
                        memcpy((char*)pageData+4088, &totalRIDOffset, sizeof(int));

                        root.pageNum = 0;
                        root.slotNum = totalRIDOffset;
                    }
                    else
                    {
                        int newOffset = node_offset-split2_offset-split1_offset;
                        int beginVal = 0;
                        int newRIDOffset = 4080;
                        memcpy((char*)node+4080, &beginVal, sizeof(int));
                        memcpy((char*)node+4084, &newOffset, sizeof(int));
                        memcpy((char*)node+4088, &newRIDOffset, sizeof(int));
                        memcpy((char*)node+4092, &newOffset, sizeof(int));

                        if(split2_rid.pageNum == 0)
                            root.pageNum = 1;
                        else
                            root.pageNum = 2;

                        root.slotNum = newRIDOffset;
                    }


                    //now we can update the "pages" and node RIDs
                    //the lowest "page" to the newest "page"
                    memcpy((char*)pageData+24, &split2_rid.pageNum, sizeof(int));
                    memcpy((char*)pageData+28, &split2_rid.slotNum, sizeof(int));

                    //the node pointers
                    //the root is on a whole new page
                    int offset_to_use = 0;
                    //all "pages" are on page 0
                    if(root.pageNum == 0 && root.slotNum == 4064)
                        offset_to_use = split2_offset;
                    //only first split in on page 0
                    else if(root.pageNum == 0 && root.slotNum == 4072)
                        offset_to_use = split1_offset;

                    if(root.pageNum == 0)
                    {
                        memcpy((char *) pageData + offset_to_use + 8, &split1_rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + offset_to_use + 12, &split1_rid.slotNum, sizeof(int));
                        if (selection == 0 || selection == 1)
                        {
                            memcpy((char *) pageData + offset_to_use + 20, &split2_rid.pageNum, sizeof(int));
                            memcpy((char *) pageData + offset_to_use + 24, &split2_rid.slotNum, sizeof(int));
                        } else
                        {
                            memcpy(&int_holder, (char *) node + 8, sizeof(int));
                            memcpy((char *) pageData + offset_to_use + 20 + int_holder, &split2_rid.pageNum,
                                   sizeof(int));
                            memcpy((char *) pageData + offset_to_use + 24 + int_holder, &split2_rid.slotNum,
                                   sizeof(int));
                        }
                    }
                    else
                    {
                        memcpy((char *) node + offset_to_use + 8, &split1_rid.pageNum, sizeof(int));
                        memcpy((char *) node + offset_to_use + 12, &split1_rid.slotNum, sizeof(int));
                        if (selection == 0 || selection == 1)
                        {
                            memcpy((char *) node + offset_to_use + 20, &split2_rid.pageNum, sizeof(int));
                            memcpy((char *) node + offset_to_use + 24, &split2_rid.slotNum, sizeof(int));
                        } else
                        {
                            memcpy(&int_holder, (char *) node + 8, sizeof(int));
                            memcpy((char *) node + offset_to_use + 20 + int_holder, &split2_rid.pageNum,
                                   sizeof(int));
                            memcpy((char *) node + offset_to_use + 24 + int_holder, &split2_rid.slotNum,
                                   sizeof(int));
                        }

                        if(ixFileHandle.appendPage(node) == -1)
                            return -1;
                    }

                    if(ixFileHandle.writePage(0, pageData) == -1)
                        return -1;

                    free(split1);
                    free(split2);
                    free(node);
                }
                else
                {
                    int return_val = ixFileHandle.appendPage(pageData);
                    free(pageData);
                    return return_val;
                }
            }

            /*******************************************
             * if we have a root to the B+ tree
             *******************************************/
            else
            {
                //read page of root
                if(ixFileHandle.readPage(root.pageNum,pageData) == -1)
                    return -1;

                //search for the leaf node to insert the data
                RID current_rid = root;
                RID next_rid;
                int begin_offset = 0;
                int end_offset = 0;
                bool found_leaf = false;
                while(!found_leaf)
                {
                    //find where the node/leaf is
                    memcpy(&begin_offset, (char*)pageData+current_rid.slotNum, sizeof(int));
                    memcpy(&end_offset, (char*)pageData+current_rid.slotNum+4, sizeof(int));

                    //check for tombstone connection
                    if(end_offset-begin_offset == 8)
                    {
                        path_to_leaf.push_back(current_rid);
                        path_is_tombstone.push_back(true);

                        memcpy(&next_rid.pageNum, (char*)pageData+begin_offset, sizeof(int));
                        memcpy(&next_rid.slotNum, (char*)pageData+begin_offset+4, sizeof(int));
                    }
                    //else read data
                    else
                    {
                        //read if is_leaf
                        memcpy(&int_holder, (char*)pageData, sizeof(int));
                        found_leaf = (int_holder == -1);
                        //if not read how many keys are in node
                        if(!found_leaf)
                        {
                            //push back current_rid to path_to_leaf
                            path_to_leaf.push_back(current_rid);
                            path_is_tombstone.push_back(false);
                            //search index-key+1 so we can determine which rid to travel too next from saved data
                            int numKeys = 0;
                            int index = 16;
                            int lesser_index = -1;
                            memcpy(&numKeys, (char*)pageData+begin_offset+4, sizeof(int));
                            for(int i = 0; i < numKeys; i++)
                            {
                                switch(selection)
                                {
                                    case 0:
                                        index = (i*12)+16;
                                        memcpy(&int_holder, (char*)pageData+begin_offset+index, sizeof(int));
                                        if(lesser_index == -1 && int_key < int_holder)
                                            lesser_index = index;
                                        break;
                                    case 1:
                                        index = (i*12)+16;
                                        memcpy(&real_holder, (char*)pageData+begin_offset+index, sizeof(float));
                                        if(lesser_index == -1 && real_key < real_holder)
                                            lesser_index = index;
                                        break;
                                    case 2:
                                        memcpy(&int_holder, (char*)pageData+begin_offset+index, sizeof(int));
                                        memcpy(str_holder, (char*)pageData+begin_offset+index+4, int_holder);
                                        if(lesser_index == -1 && str_key < str_holder)
                                            lesser_index = index;
                                        index += 12 + int_holder;
                                        break;
                                }
                            }

                            //re-read new page if necsisary
                            if(lesser_index != -1)
                            {
                                memcpy(&next_rid.pageNum, (char*)pageData+begin_offset+lesser_index-8, sizeof(float));
                                memcpy(&next_rid.slotNum, (char*)pageData+begin_offset+lesser_index-4, sizeof(float));
                            }
                            else
                            {
                                if(selection == 0 || selection == 1)
                                {
                                    memcpy(&next_rid.pageNum, (char *) pageData+begin_offset + index + 4, sizeof(float));
                                    memcpy(&next_rid.slotNum, (char *) pageData+begin_offset + index + 8, sizeof(float));
                                }
                                else
                                {
                                    memcpy(&int_holder, (char*)pageData+index, sizeof(int));
                                    memcpy(&next_rid.pageNum, (char *) pageData+begin_offset + index + 4 + int_holder, sizeof(float));
                                    memcpy(&next_rid.slotNum, (char *) pageData+begin_offset + index + 8 + int_holder, sizeof(float));
                                }
                            }

                        }
                    }

                    if(current_rid.pageNum != next_rid.pageNum)
                    {
                        if(ixFileHandle.readPage(next_rid.pageNum,pageData) == -1)
                            return -1;
                    }

                    //set current_rid to next_rid
                    current_rid = next_rid;
                }

                /************************************
                 * Check leaf node values
                 ************************************/
                int totalPageOffset = 0;
                int totalRIDOffset = 0;
                memcpy(&totalPageOffset, (char*)pageData+4092, sizeof(int));
                memcpy(&totalRIDOffset, (char*)pageData+4086, sizeof(int));

                //read the leaf "page" as we do above
                //as long as "page" is not full, updates should be trivial
                int leafOffset = 0;
                int leafEndOffset = 0;
                memcpy(&leafOffset, (char*)pageData+current_rid.slotNum, sizeof(int));
                memcpy(&leafEndOffset, (char*)pageData+current_rid.slotNum+4, sizeof(int));

                int offsets[5];
                int filled_values = 0;
                int add_index = 0;
                //check all keys for match first:
                for(int i = 0; i < 4; i++)
                {
                    memcpy(&offsets[i], (char*)pageData+leafOffset+((i*4) + 4), sizeof(int));
                    memcpy(&offsets[i+1], (char*)pageData+leafOffset+(((i+1)*4) + 4), sizeof(int));
                    if (offsets[i+1] != -1)
                        filled_values += 1;
                    else if(add_index == 0)
                        add_index = i;
                }


                //first check if a our insert key is lesser than any existing keys (these should already be in sorted order)
                int index = 0;
                bool found_earlier_slot = false;
                for(; index < 4; index++)
                {
                    //check if we have another value
                    if(offsets[index+1] == -1)
                        break;

                    switch(selection)
                    {
                        case 0:
                            memcpy(&int_holder, (char*)pageData+leafOffset+offsets[index], sizeof(int));
                            found_earlier_slot = int_key <= int_holder;
                            break;
                        case 1:
                            memcpy(&real_holder, (char*)pageData+leafOffset+offsets[index], sizeof(float));
                            found_earlier_slot = real_key <= real_holder;
                            break;
                        case 2:
                            memcpy(&int_holder, (char*)pageData+leafOffset+offsets[index], sizeof(int));
                            memcpy(str_holder, (char*)pageData+leafOffset+offsets[index]+4, int_holder);
                            found_earlier_slot = str_key <= str_holder;
                            break;
                    }

                    //if the keys are the same also break before we update index
                    if(found_earlier_slot)
                        break;
                }

                //if we found an earlier slot to insert into, we need to shift everything and update the values
                if(selection == 0 || selection == 1)
                {
                    if (found_earlier_slot)
                    {
                        //do the shift
                        memcpy(&int_holder, (char *) pageData + current_rid.slotNum + 4, sizeof(int));
                        memcpy((char *) pageData + leafOffset + (offsets[index] + 12),
                               (char *) pageData + leafOffset + (offsets[index]), (int_holder - offsets[index]));

                        //add new values
                        memcpy((char *) pageData + leafOffset + offsets[index], (char *) key, sizeof(int));
                        memcpy((char *) pageData + leafOffset + offsets[index] + 4, &rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + leafOffset + offsets[index] + 8, &rid.slotNum, sizeof(int));

                        //update the offsets of the "page"
                        for (int update_index = index; update_index < 4; update_index++)
                        {
                            if (offsets[update_index] != -1)
                            {
                                int_holder = offsets[update_index] + 12;
                                memcpy((char *) pageData + leafOffset + (((update_index + 1) * 4) + 4), &int_holder,
                                       sizeof(int));
                            }
                        }
                    }
                    //else the insert is very easy
                    else
                    {
                        //add new values
                        memcpy((char *) pageData + leafOffset + offsets[add_index], (char *) key, sizeof(int));
                        memcpy((char *) pageData + leafOffset + offsets[add_index] + 4, &rid.pageNum, sizeof(int));
                        memcpy((char *) pageData + leafOffset + offsets[add_index] + 8, &rid.slotNum, sizeof(int));

                        //update the offsets of the "page"
                        int_holder = offsets[add_index + 1] + 12;
                        memcpy((char *) pageData + leafOffset + (((add_index + 1) * 4) + 4), &int_holder, sizeof(int));
                    }

                    if(ixFileHandle.writePage(current_rid.pageNum, pageData) == -1)
                        return -1;
                }

                /************************************************
                 *  VarChar Insertation.
                 ************************************************/
                else
                {
                    //if we have space on the page
                    if(int_key+12 <= totalRIDOffset-totalPageOffset-8)
                    {
                        if (found_earlier_slot)
                        {
                            //do the shift
                            memcpy((char *) pageData + leafOffset + (offsets[index] + int_key+12),
                                   (char *) pageData + leafOffset + (offsets[index]), (totalPageOffset - offsets[index]));

                            //add new values
                            memcpy((char *) pageData + leafOffset + offsets[index], &int_key, sizeof(int));
                            memcpy((char *) pageData + leafOffset + offsets[index] + 4, str_key, int_key);
                            memcpy((char *) pageData + leafOffset + offsets[index] + 4 +int_key, &rid.pageNum, sizeof(int));
                            memcpy((char *) pageData + leafOffset + offsets[index] + 8 +int_key, &rid.slotNum, sizeof(int));

                            //update the offsets of the "page"
                            for (int update_index = index; update_index < 4; update_index++)
                            {
                                if (offsets[update_index] != -1) {
                                    int_holder = offsets[update_index] + 12 + int_key;
                                    memcpy((char *) pageData + leafOffset + (((update_index + 1) * 4) + 4), &int_holder,
                                           sizeof(int));
                                }
                            }
                        }
                        //else the insert is very easy
                        else
                        {
                            //do the shift
                            memcpy((char *) pageData + leafOffset + (offsets[add_index] + int_key+12),
                                   (char *) pageData + leafOffset + (offsets[add_index]), (totalPageOffset - offsets[add_index]));

                            //add new values
                            memcpy((char *) pageData + leafOffset + offsets[add_index], &int_key, sizeof(int));
                            memcpy((char *) pageData + leafOffset + offsets[add_index] + 4, str_key, int_key);
                            memcpy((char *) pageData + leafOffset + offsets[add_index] + 4 +int_key, &rid.pageNum, sizeof(int));
                            memcpy((char *) pageData + leafOffset + offsets[add_index] + 8 +int_key, &rid.slotNum, sizeof(int));

                            //update the offsets of the "page"
                            int_holder = offsets[add_index + 1] + 12 + int_key;
                            memcpy((char *) pageData + leafOffset + (((add_index + 1) * 4) + 4), &int_holder,
                                   sizeof(int));
                        }

                        //update offsets at end of page
                        for(int i = 4084; i >= totalRIDOffset; i-= 4)
                        {
                            memcpy(&int_holder, (char*)pageData+i, sizeof(int));
                            if(int_holder > leafOffset)
                            {
                                int_holder += 12 + int_key;
                                memcpy((char*)pageData+i, &int_holder, sizeof(int));
                            }
                        }

                        totalPageOffset += 12 + int_key;
                        memcpy((char*)pageData+4092, &totalPageOffset, sizeof(int));

                        if(ixFileHandle.writePage(current_rid.pageNum, pageData) == -1)
                            return -1;
                    }

                    /*****************************************
                     *  More pages need on insert
                     ****************************************/
                    //else we do not have space on the page
                    else
                    {
                        int totalPageOffset_2 = 0;
                        int totalRIDOffset_2 = 4080;
                        void* pageData2 = malloc(4096);

                        bool pageFound = false;
                        int updated_page = current_rid.pageNum;
                        //first check last page for space before scanning
                        if(current_rid.pageNum != ixFileHandle.ixAppendPageCounter-1)
                        {
                            if(ixFileHandle.readPage(ixFileHandle.ixAppendPageCounter-1, pageData2) == -1)
                                return -1;

                            memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                            memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                            if(((leafEndOffset-leafOffset)+int_key+12) <= totalRIDOffset_2-totalPageOffset_2-8)
                            {
                                pageFound = true;
                                updated_page = ixFileHandle.ixAppendPageCounter-1;
                            }
                        }
                        //scan for free space in all pages
                        else if(!pageFound)
                        {
                            for(int i = 0; i < ixFileHandle.ixAppendPageCounter-2 && !pageFound; i++)
                            {
                                if(ixFileHandle.readPage(i, pageData2) == -1)
                                    return -1;

                                memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                if(((leafEndOffset-leafOffset)+int_key+12) <= totalRIDOffset_2-totalPageOffset_2-8)
                                {
                                    pageFound = true;
                                    updated_page = i;
                                }
                            }
                        }

                        int newBeginOffset = totalPageOffset_2;
                        RID new_tombstone;
                        //if we found a page to hold the data
                        if(pageFound)
                        {
                            //update offsets and reduce prevous current page to a tombstone
                            totalPageOffset_2 += (leafEndOffset-leafOffset) + 12 + int_key;
                            totalRIDOffset_2 -= 8;
                            //first update new data to be put into the second page
                            memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                            memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                            new_tombstone.pageNum = updated_page;
                            new_tombstone.slotNum = totalRIDOffset_2;
                        }
                        //else we need to append a page
                        else
                        {
                            //update offsets and reduce prevous current page to a tombstone
                            newBeginOffset = 0;
                            totalPageOffset_2 = (leafEndOffset-leafOffset) + 12 + int_key;
                            totalRIDOffset_2 = 4080;
                            //first update new data to be put into the second page
                            memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                            memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                            new_tombstone.pageNum = ixFileHandle.ixAppendPageCounter;
                            new_tombstone.slotNum = totalRIDOffset_2;
                        }

                        //add new record of second page
                        memcpy((char*)pageData2+totalRIDOffset_2, &newBeginOffset ,sizeof(int));
                        memcpy((char*)pageData2+totalRIDOffset_2+4, &totalPageOffset_2 ,sizeof(int));

                        //copy "page" from old page to new page (will add the new value later below)
                        memcpy((char*)pageData2+newBeginOffset, (char*)pageData+leafOffset, (leafEndOffset-leafOffset));

                        //put tombstone in old page and compress page
                        memcpy((char*)pageData+leafOffset, &new_tombstone.pageNum, sizeof(int));
                        memcpy((char*)pageData+leafOffset, &new_tombstone.slotNum, sizeof(int));

                        memcpy((char*)pageData+leafOffset+8, (char*)pageData+leafEndOffset, (totalPageOffset-leafEndOffset));
                        totalPageOffset -= (leafEndOffset-leafOffset)+8;
                        memcpy((char*)pageData+4092, &totalPageOffset, sizeof(int));

                        for(int i = 4084; i >= totalRIDOffset; i-= 4)
                        {
                            memcpy(&int_holder, (char*)pageData+i, sizeof(int));
                            if(int_holder > leafOffset)
                            {
                                int_holder -= (leafEndOffset-leafOffset)+8;
                                memcpy((char*)pageData+i, &int_holder, sizeof(int));
                            }
                        }

                        //we can skip updating record values here since we already have a appended a page and know
                        //that there is only useable free space after our data
                        if (found_earlier_slot)
                        {
                            //do the shift
                            memcpy((char *) pageData2 + newBeginOffset + (offsets[index] + int_key+12),
                                   (char *) pageData2 + newBeginOffset + (offsets[index]), (totalPageOffset - offsets[index]));

                            //add new values
                            memcpy((char *) pageData2 + newBeginOffset + offsets[index], &int_key, sizeof(int));
                            memcpy((char *) pageData2 + newBeginOffset + offsets[index] + 4, str_key, int_key);
                            memcpy((char *) pageData2 + newBeginOffset + offsets[index] + 4 +int_key, &rid.pageNum, sizeof(int));
                            memcpy((char *) pageData2 + newBeginOffset + offsets[index] + 8 +int_key, &rid.slotNum, sizeof(int));

                            //update the offsets of the "page"
                            for (int update_index = index; update_index < 4; update_index++)
                            {
                                if (offsets[update_index] != -1) {
                                    int_holder = offsets[update_index] + 12 + int_key;
                                    memcpy((char *) pageData2 + newBeginOffset + (((update_index + 1) * 4) + 4), &int_holder,
                                           sizeof(int));
                                }
                            }
                        }
                        //else the insert is very easy
                        else
                        {
                            //add new values
                            memcpy((char *) pageData2 + leafOffset + offsets[add_index], &int_key, sizeof(int));
                            memcpy((char *) pageData2 + leafOffset + offsets[add_index] + 4, str_key, int_key);
                            memcpy((char *) pageData2 + leafOffset + offsets[add_index] + 4 +int_key, &rid.pageNum, sizeof(int));
                            memcpy((char *) pageData2 + leafOffset + offsets[add_index] + 8 +int_key, &rid.slotNum, sizeof(int));

                            //update the offsets of the "page"
                            int_holder = offsets[add_index + 1] + 12 + int_key;
                            memcpy((char *) pageData2 + leafOffset + (((add_index + 1) * 4) + 4), &int_holder,
                                   sizeof(int));
                        }

                        //write(pageData)
                        if(ixFileHandle.writePage(current_rid.pageNum, pageData) == -1)
                            return -1;

                        //write Pagedata2 (also re-read pageData2 to pageData and free pageData2)
                        if(pageFound)
                        {
                            if(ixFileHandle.writePage(new_tombstone.pageNum, pageData2) == -1)
                                return -1;
                        }
                        //append Pagedata2 (also re-read pageData2 to pageData and free pageData2)
                        else
                        {
                            if(ixFileHandle.appendPage(pageData2) == -1)
                                return -1;
                        }

                        if(ixFileHandle.readPage(new_tombstone.pageNum, pageData) == -1)
                            return -1;

                        free(pageData2);

                        //we will want to update the current rid and path of rids after we generate do all this
                        path_to_leaf.push_back(current_rid);
                        path_is_tombstone.push_back(true);
                        current_rid = new_tombstone;
                    }
                }

                /***************************************************************
                 *  The actually split logic
                 ***************************************************************/
                //if our "page" is full we need to split it.
                bool needs_split = true;
                for(int i; i < 5; i++)
                {
                    memcpy(&int_holder, (char*)pageData+((i*4) + 4), sizeof(int));
                    if(needs_split && int_holder == -1)
                    {
                        needs_split = false;
                        break;
                    }
                }

                //here we will do the actual split
                if(needs_split)
                {
                    //if we need to split the "page" we need to check back recussively up the path_to_leaf vector
                    //in order to accound for splitting the nodes, we will also need to account for splitting root node
                    //though setting up the two new "pages" should be similar to what we did above

                    //first though we need to split the leafnodes
                    //TODO:INCORPERATE A SYSTEM TO ELIMINATE TOMBSTONES (current design will leave sudo-dead-tombstones)
                    RID node_rid;
                    RID selected_tomb_rid; //the rid saved in the node which points to the tombstone which will end up pointing to our current_rid
                    selected_tomb_rid.pageNum = -1;
                    selected_tomb_rid.slotNum = -1;

                    bool is_tombstone = true;
                    do
                    {
                        node_rid = path_to_leaf.back();
                        is_tombstone = path_is_tombstone.back();
                        path_to_leaf.pop_back();
                        path_is_tombstone.pop_back();
                        if(is_tombstone)
                            selected_tomb_rid = node_rid;
                    }while(is_tombstone);

                    int empty_val_indicator = -1;

                    //this is over kill on the malloc but its nice to have wiggle room
                    void* split1 = malloc(4096);
                    void* split2 = malloc(4096);
                    void* node = malloc(4096);

                    //we may not need these pages, but as there are case we will reserve these extra pages
                    void* pageData2 = malloc(4096);
                    void* node2 = malloc(4096);
//                    void* moveUPnode = malloc(4096);

                    int split1_offset = 0;
                    int split2_offset = 0;
                    int node_offset = 0; //dont need this for right now but when dealing with varchars yes

                    int sudoPageBegin = 0;
                    int sudoPageEnd = 0;

                    memcpy(&sudoPageBegin, (char*)pageData+current_rid.slotNum, sizeof(int));
                    memcpy(&sudoPageEnd, (char*)pageData+current_rid.slotNum+4, sizeof(int));

                    //copy first half of "page" into temp memory. (also set last two offsets to -1
                    int first_offset = 0;
                    memcpy(&first_offset, (char*)pageData+sudoPageBegin+12, sizeof(int));
                    memcpy((char*)split1, (char*)pageData+sudoPageBegin, first_offset);
                    memcpy((char*)split1+16, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+20, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+24, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split1+28, &empty_val_indicator, sizeof(int));
                    if(selection == 0 || selection == 1)
                        split1_offset += 64; //manually setting this for ints/real
                    else
                        split1_offset = first_offset;

                    //now we carefully copy the second half of the "page"
                    int second_offset = 0;
                    int third_offset = 0;
                    memcpy(&first_offset, (char*)pageData+sudoPageBegin+12, sizeof(int));
                    memcpy(&second_offset, (char*)pageData+sudoPageBegin+16, sizeof(int));
                    memcpy(&third_offset, (char*)pageData+sudoPageBegin+20, sizeof(int));

                    int first_entry = 32; //standard first entry offset
                    int second_entry = first_entry + (second_offset-first_offset);
                    int third_entry = second_entry + (third_offset-second_offset);

                    memcpy((char*)split2, (char*)pageData, sizeof(int));
                    memcpy((char*)split2+4, &first_entry, sizeof(int));
                    memcpy((char*)split2+8, &second_entry, sizeof(int));
                    memcpy((char*)split2+12, &third_entry, sizeof(int));
                    memcpy((char*)split2+16, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+20, &empty_val_indicator, sizeof(int));
                    memcpy((char*)split2+24, (char*)pageData+sudoPageBegin+24, sizeof(int));
                    memcpy((char*)split2+28, (char*)pageData+sudoPageBegin+28, sizeof(int));
                    memcpy((char*)split2+32, (char*)pageData+first_offset, (third_offset-first_offset));
                    if(selection == 0 || selection == 1)
                        split2_offset += 64+split1_offset; //manually setting this for ints/real
                    else
                        split2_offset = third_entry + split1_offset;

                    //all data (execpt for the node) has been added so now we need to update our files data
                    //first we will add the lesser split as it will take up less space than the "page" in memory
                    //followed by the other half so we can point the first new "page" to the second
                    //then lastly the node so we can point the node to newly created "pages"

                    //we have three in case we need extra for the extra pages. First we should compact the page
                    //with the left split of the "page"
                    int totalPageOffset_1 = 0;
                    int totalRIDOffset_1 = 0;

                    int totalPageOffset_2 = 0;
                    int totalRIDOffset_2 = 0;

                    //first we can compact the first "page" (this part is mostly if we have varchar elements)
                    memcpy(&totalPageOffset_1, (char*)pageData+4092, sizeof(int));
                    memcpy(&totalRIDOffset_1, (char*)pageData+4088, sizeof(int));
                    memcpy((char*)pageData+sudoPageBegin+split1_offset, (char*)pageData+sudoPageEnd, (totalPageOffset_1-sudoPageEnd));
                    for(int i = 4084; i >= totalRIDOffset_1; i-=4)
                    {
                        memcpy(&int_holder, (char*)pageData+i, sizeof(int));
                        if(int_holder >= sudoPageEnd)
                        {
                            int_holder -= sudoPageEnd-(sudoPageBegin+split1_offset);
                            memcpy((char*)pageData+i, &int_holder, sizeof(int));
                        }
                    }

                    //memory for non varchars should already be reserved. So we only have to do this for varchars
                    if(selection==2)
                    {
                        totalPageOffset_1 -= (sudoPageEnd-(split1_offset+sudoPageBegin));
                        memcpy((char*)pageData+4092, &totalPageOffset_1, sizeof(int));
                    }

                    //now we check if we can check if the second leaf node fits on the current page
                    //we will check non-leaf nodes after since we may have to split those recursively
                    bool pageFound = false;
                    int updated_split2_page = -1;
                    //first check last page for space before scanning

                    if((split2_offset-split1_offset) <= totalRIDOffset_1-totalPageOffset_1-8)
                    {
                        pageFound = true;
                        updated_split2_page = current_rid.pageNum;
                    }
                    else if(!pageFound && current_rid.pageNum != ixFileHandle.ixAppendPageCounter-1)
                    {
                        if(ixFileHandle.readPage(ixFileHandle.ixAppendPageCounter-1, pageData2) == -1)
                            return -1;

                        memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                        memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                        if((split2_offset-split1_offset) <= totalRIDOffset_2-totalPageOffset_2-8)
                        {
                            pageFound = true;
                            updated_split2_page = ixFileHandle.ixAppendPageCounter-1;
                        }
                    }
                    //scan for free space in all pages
                    else if(!pageFound)
                    {
                        for(int i = 0; i < ixFileHandle.ixAppendPageCounter-2 && !pageFound; i++)
                        {
                            if(ixFileHandle.readPage(i, pageData2) == -1)
                                return -1;

                            memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                            memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                            if((split2_offset-split1_offset) <= totalRIDOffset_2-totalPageOffset_2-8)
                            {
                                pageFound = true;
                                updated_split2_page = i;
                            }
                        }
                    }

                    int newBeginOffset = totalPageOffset_2;
                    RID split2_rid;
                    //if we found a page to hold the data
                    if(pageFound && updated_split2_page == current_rid.pageNum)
                    {
                        //update offsets
                        newBeginOffset = totalPageOffset_1;
                        totalPageOffset_1 += (split2_offset-split1_offset);
                        totalRIDOffset_1 -= 8;
                        //first update new data to be put into the second page
                        memcpy((char*)pageData+4092, &totalPageOffset_1 ,sizeof(int));
                        memcpy((char*)pageData+4088, &totalRIDOffset_1 ,sizeof(int));

                        split2_rid.pageNum = updated_split2_page;
                        split2_rid.slotNum = totalRIDOffset_1;

                    }
                    else if(pageFound)
                    {
                        //update offsets
                        totalPageOffset_2 += (split2_offset-split1_offset);
                        totalRIDOffset_2 -= 8;
                        //first update new data to be put into the second page
                        memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                        memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                        split2_rid.pageNum = updated_split2_page;
                        split2_rid.slotNum = totalRIDOffset_2;
                    }
                    //else we need to append a page
                    else
                    {
                        //update offsets
                        newBeginOffset = 0;
                        totalPageOffset_2 = (split2_offset-split1_offset);
                        totalRIDOffset_2 = 4080;
                        //first update new data to be put into the second page
                        memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                        memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                        split2_rid.pageNum = ixFileHandle.ixAppendPageCounter;
                        split2_rid.slotNum = totalRIDOffset_2;
                    }

                    //for the connection to the next "page" for scanning we add the split2's rid to the split1
                    memcpy((char*)pageData+sudoPageBegin+24, &split2_rid.pageNum, sizeof(int));
                    memcpy((char*)pageData+sudoPageBegin+28, &split2_rid.slotNum, sizeof(int));

                    //we want use pageData
                    if(updated_split2_page == current_rid.pageNum)
                    {
                        //add new record of second page
                        memcpy((char*)pageData+totalRIDOffset_1, &newBeginOffset ,sizeof(int));
                        memcpy((char*)pageData+totalRIDOffset_1+4, &totalPageOffset_1 ,sizeof(int));

                        //copy "page" from old page to new page (will add the new value later below)
                        memcpy((char*)pageData+newBeginOffset, (char*)split2, (split2_offset - split1_offset));

                        //write(pageData)
                        if(ixFileHandle.writePage(current_rid.pageNum, pageData) == -1)
                            return -1;
                    }
                    //else we want to use pageData2
                    else
                    {
                        //add new record of second page
                        memcpy((char*)pageData2+totalRIDOffset_2, &newBeginOffset ,sizeof(int));
                        memcpy((char*)pageData2+totalRIDOffset_2+4, &totalPageOffset_2 ,sizeof(int));

                        //copy "page" from old page to new page (will add the new value later below)
                        memcpy((char*)pageData2+newBeginOffset, (char*)split2, (split2_offset - split1_offset));

                        //write(pageData)
                        if(ixFileHandle.writePage(current_rid.pageNum, pageData) == -1)
                            return -1;

                        //write(pageData2)
                        if(pageFound)
                        {
                            if (ixFileHandle.writePage(split2_rid.pageNum, pageData2) == -1)
                                return -1;
                        }
                        //else append(pageData2)
                        else
                        {
                            if (ixFileHandle.appendPage(pageData2) == -1)
                                return -1;
                        }
                    }

                    //-----------------------------------------------------------------------------------------------
                    //now that the leaf node is in place we work on the non-leaf nodes
                    int active_keys = 1;
                    bool need_to_split_node = false;
                    //first we need to load the node if not already here
                    int nodeBegin = 0;
                    int nodeEnd = 0;

                    //get new key before entering loop so we can reaquire if we need to split on a node
                    switch(selection)
                    {
                        case 0:
                            memcpy(&int_key, (char*)split2+first_entry, sizeof(int));
                            break;
                        case 1:
                            memcpy(&real_key, (char*)split2+first_entry, sizeof(float));
                            break;
                        case 2:
                            memcpy(&int_key, (char*)split2+first_entry, sizeof(int));
                            memcpy(str_key, (char*)split2+first_entry+4, int_key);
                            break;
                    }

                    do{
                        need_to_split_node = false;
                        bool is_root = (node_rid.pageNum == root.pageNum && node_rid.slotNum == root.slotNum);

                        if(ixFileHandle.readPage(node_rid.pageNum,pageData) == -1)
                            return -1;

                        memcpy(&nodeBegin, (char*)pageData+node_rid.slotNum, sizeof(int));
                        memcpy(&nodeEnd, (char*)pageData+node_rid.slotNum+4, sizeof(int));
                        memcpy((char*)node, (char*)pageData+nodeBegin, (nodeEnd-nodeBegin));

                        node_offset = nodeEnd-nodeBegin;
                        memcpy(&active_keys, (char*)node+4, sizeof(int));
                        active_keys++;
                        if (active_keys>3)
                            need_to_split_node = true;
                        else
                            memcpy((char*)node+4, &active_keys, sizeof(int));

                        //before editing the node we will remove it from the page and compact it for fresh space
                        //"clean-ish", I'll be leaving "deleted Records" since it wont really break anything and
                        //I'm on a time crunch (though I will repopulate the rid if there is space on the page for the
                        //node to be placed back here)
                        memcpy(&totalRIDOffset_1, (char*)pageData+9088, sizeof(int));
                        memcpy(&totalPageOffset_1, (char*)pageData+9092, sizeof(int));
                        memcpy((char*)pageData+nodeBegin, (char*)pageData+nodeEnd, (totalPageOffset_1-nodeEnd));
                        totalPageOffset_1 -= node_offset;
                        memcpy((char*)pageData+9092, &totalPageOffset_1, sizeof(int));
                        memcpy((char*)pageData+node_rid.slotNum, &empty_val_indicator, sizeof(int));
                        memcpy((char*)pageData+node_rid.slotNum+4, &empty_val_indicator, sizeof(int));

                        for(int i = 4084; i >= totalRIDOffset_1; i-=4)
                        {
                            memcpy(&int_holder, (char*)pageData+i, sizeof(int));
                            if(int_holder >= nodeEnd)
                            {
                                int_holder -= node_offset;
                                memcpy((char*)pageData+i, &int_holder, sizeof(int));
                            }
                        }

                        //find where to put the new key (by RID)
                        int index = -1;
                        int slot = -1;
                        for(int i = 0, i2 =0; i<active_keys; i++)
                        {
                            switch(selection)
                            {
                                case 0:
                                    i2 = ((i*12) + 8);
                                    memcpy(&int_holder, (char*)node+i2, sizeof(int));
                                    if(selected_tomb_rid.pageNum = -1)
                                    {
                                        if(int_holder == current_rid.pageNum)
                                            break;
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != current_rid.slotNum)
                                            break;
                                    }
                                    else
                                    {
                                        if(int_holder == selected_tomb_rid.pageNum)
                                            break;
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != selected_tomb_rid.slotNum)
                                            break;
                                    }
                                    index = i2;
                                    slot = i;
                                    break;
                                case 1:
                                    i2 = ((i*12) + 8);
                                    memcpy(&int_holder, (char*)node+i2, sizeof(int));
                                    if(selected_tomb_rid.pageNum = -1)
                                    {
                                        if(int_holder == current_rid.pageNum)
                                            break;
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != current_rid.slotNum)
                                            break;
                                    }
                                    else
                                    {
                                        if(int_holder == selected_tomb_rid.pageNum)
                                            break;
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != selected_tomb_rid.slotNum)
                                            break;
                                    }
                                    index = i2;
                                    slot = i;
                                    break;
                                case 2:
                                    if(i == 0)
                                        i2 = 8;
                                    memcpy(&int_holder, (char*)node+i2, sizeof(int));
                                    if(selected_tomb_rid.pageNum = -1)
                                    {
                                        if(int_holder == current_rid.pageNum)
                                        {
                                            memcpy(&int_holder, (char*)node+i2+8, sizeof(int));
                                            i2 += 12 + int_holder;
                                            break;
                                        }
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != current_rid.slotNum)
                                        {
                                            memcpy(&int_holder, (char*)node+i2+8, sizeof(int));
                                            i2 += 12 + int_holder;
                                            break;
                                        }
                                    }
                                    else
                                    {
                                        if(int_holder == selected_tomb_rid.pageNum)
                                        {
                                            memcpy(&int_holder, (char*)node+i2+8, sizeof(int));
                                            i2 += 12 + int_holder;
                                            break;
                                        }
                                        memcpy(&int_holder, (char*)node+i2+4, sizeof(int));
                                        if(int_holder != selected_tomb_rid.slotNum)
                                        {
                                            memcpy(&int_holder, (char*)node+i2+8, sizeof(int));
                                            i2 += 12 + int_holder;
                                            break;
                                        }
                                    }
                                    index = i2;
                                    slot = i;
                                    break;
                            }

                            if(index != -1 && slot != -1)
                                break;
                        }

                        //now we have to move all memory to the right if the rid is not the far most right index
                        //we will reuse the current_rid and split2_rid variables here as the left and right indexs respectively
                        switch(selection)
                        {
                            case 0:
                                if(index != nodeEnd-8)
                                    memcpy((char*)node+index+12, (char*)node+index+8, (nodeEnd-index+8));

                                memcpy((char*)node+index, &current_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+4, &current_rid.slotNum, sizeof(int));
                                memcpy((char*)node+index+8, &int_key, sizeof(int));
                                memcpy((char*)node+index+12, &split2_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+16, &split2_rid.slotNum, sizeof(int));
                                node_offset+=20;
                                break;
                            case 1:
                                if(index != nodeEnd-8)
                                    memcpy((char*)node+index+12, (char*)node+index+8, (nodeEnd-index+8));

                                memcpy((char*)node+index, &current_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+4, &current_rid.slotNum, sizeof(int));
                                memcpy((char*)node+index+8, &int_key, sizeof(float));
                                memcpy((char*)node+index+12, &split2_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+16, &split2_rid.slotNum, sizeof(int));
                                node_offset+=20;
                                break;
                            case 2:
                                if(index != nodeEnd-8)
                                    memcpy((char*)node+index+12+int_key, (char*)node+index+8, (nodeEnd-index+8));

                                memcpy((char*)node+index, &current_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+4, &current_rid.slotNum, sizeof(int));
                                memcpy((char*)node+index+8, &int_key, sizeof(int));
                                memcpy((char*)node+index+12, str_key, int_key);
                                memcpy((char*)node+index+12+int_key, &split2_rid.pageNum, sizeof(int));
                                memcpy((char*)node+index+16+int_key, &split2_rid.slotNum, sizeof(int));
                                node_offset+=20+int_key;
                                break;
                        }

                        //now we can insert the node back into the page it has come from or put it in a new page/append
                        if(!need_to_split_node)
                        {
                            bool pageFound = false;
                            int updated_node_page = -1;
                            int pageData_pageNum = node_rid.pageNum;
                            //first check last page for space before scanning

                            if(node_offset <= totalRIDOffset_1-totalPageOffset_1)
                            {
                                pageFound = true;
                                updated_node_page = node_rid.pageNum;
                            }
                            else if(!pageFound && node_rid.pageNum != ixFileHandle.ixAppendPageCounter-1)
                            {
                                if(ixFileHandle.readPage(ixFileHandle.ixAppendPageCounter-1, pageData2) == -1)
                                    return -1;

                                memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                if(node_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                {
                                    pageFound = true;
                                    updated_node_page = ixFileHandle.ixAppendPageCounter-1;
                                }
                            }
                                //scan for free space in all pages
                            else if(!pageFound)
                            {
                                for(int i = 0; i < ixFileHandle.ixAppendPageCounter-2 && !pageFound; i++)
                                {
                                    if(ixFileHandle.readPage(i, pageData2) == -1)
                                        return -1;

                                    memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                    memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                    if(node_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                    {
                                        pageFound = true;
                                        updated_node_page = i;
                                    }
                                }
                            }

                            newBeginOffset = totalPageOffset_2;
                            //if we found a page to hold the data
                            if(pageFound && updated_node_page == node_rid.pageNum)
                            {
                                //update offsets
                                newBeginOffset = totalPageOffset_1;
                                totalPageOffset_1 += node_offset;
                                //first update new data to be put into page
                                memcpy((char*)pageData+4092, &totalPageOffset_1 ,sizeof(int));
                            }
                            else if(pageFound)
                            {
                                //update offsets
                                totalPageOffset_2 += node_offset;
                                totalRIDOffset_2 -= 8;
                                //first update new data to be put into the second page
                                memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                node_rid.pageNum = updated_node_page;
                                node_rid.slotNum = totalRIDOffset_2;
                            }
                            //else we need to append a page
                            else
                            {
                                //update offsets
                                newBeginOffset = 0;
                                totalPageOffset_2 = node_offset;
                                totalRIDOffset_2 = 4080;
                                //first update new data to be put into the second page
                                memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                node_rid.pageNum = ixFileHandle.ixAppendPageCounter;
                                node_rid.slotNum = totalRIDOffset_2;
                            }

                            //we want use pageData
                            if(updated_node_page == node_rid.pageNum)
                            {
                                //add new record of second page
                                memcpy((char*)pageData+node_rid.slotNum, &newBeginOffset ,sizeof(int));
                                memcpy((char*)pageData+node_rid.slotNum+4, &totalPageOffset_1 ,sizeof(int));

                                //copy "page" from old page to new page (will add the new value later below)
                                memcpy((char*)pageData+newBeginOffset, (char*)node, node_offset);

                                //write(pageData)
                                if(ixFileHandle.writePage(node_rid.pageNum, pageData) == -1)
                                    return -1;
                            }
                            //else we want to use pageData2
                            else
                            {
                                //add new record of second page
                                memcpy((char*)pageData2+totalRIDOffset_2, &newBeginOffset ,sizeof(int));
                                memcpy((char*)pageData2+totalRIDOffset_2+4, &totalPageOffset_2 ,sizeof(int));

                                //copy "page" from old page to new page (will add the new value later below)
                                memcpy((char*)pageData2+newBeginOffset, (char*)node, node_offset);

                                //write(pageData)
                                if(ixFileHandle.writePage(pageData_pageNum, pageData) == -1)
                                    return -1;

                                //write(pageData2)
                                if(pageFound)
                                {
                                    if (ixFileHandle.writePage(node_rid.pageNum, pageData2) == -1)
                                        return -1;
                                }
                                //else append(pageData2)
                                else
                                {
                                    if (ixFileHandle.appendPage(pageData2) == -1)
                                        return -1;
                                }

                                if(is_root)
                                {
                                    root.pageNum = node_rid.pageNum;
                                    root.slotNum = node_rid.slotNum;
                                }
                            }
                        }
                        //if we need to split the node and bring it back up the data this is where we will do that
                        else
                        {
                            //first get index to split on
                            int index_move = 40;
                            switch(selection)
                            {
                                case 0:
                                    memcpy(&int_key, (char*)node+index, sizeof(int));
                                    break;
                                case 1:
                                    memcpy(&real_key, (char*)node+index, sizeof(float));
                                    break;
                                case 2:
                                    for(int i = 0, index = 16; i < 2; i++)
                                    {
                                        memcpy(&int_key, (char*)node+index, sizeof(int));
                                        memcpy(str_key, (char*)node+index+4, int_key);
                                        index += 12 + int_holder;
                                    }
                                    break;
                            }

                            int node1_active_keys = 2;
                            int node2_active_keys = 1;

                            memcpy((char*)node, &empty_val_indicator, sizeof(int));
                            memcpy((char*)node+4, &node1_active_keys, sizeof(int));

                            memcpy((char*)node2, &empty_val_indicator, sizeof(int));
                            memcpy((char*)node2+4, &node2_active_keys, sizeof(int));

                            //move data into node2
                            int node2_offset = node_offset-(index_move+4);
                            memcpy((char*)node2+8, (char*)node+index_move+4, node2_offset);
                            node2_offset += 8; //to account for the first two indicaiton values

                            //update new node offset
                            node_offset -= (node_offset-(index_move));

                            //next we can directly save the new right hand node to pageData as it should be less
                            //than or equal to before the splitting
                            memcpy((char*)pageData+totalPageOffset_1, (char*)node, node_offset);
                            memcpy((char*)pageData+node_rid.slotNum, &totalPageOffset_1, sizeof(int));
                            totalPageOffset_1 += node_offset;
                            memcpy((char*)pageData+4092, &totalPageOffset_1, sizeof(int));
                            memcpy((char*)pageData+node_rid.slotNum+4, &totalPageOffset_1, sizeof(int));

                            //now we check about where to save node2
                            bool pageFound = false;
                            int updated_node_page = -1;
                            int pageData_pageNum = node_rid.pageNum;
                            RID node2_rid;

                            //check current page
                            if(node2_offset <= totalRIDOffset_1-totalPageOffset_1)
                            {
                                pageFound = true;
                                updated_node_page = node_rid.pageNum;
                            }
                            //check last page for space before scanning
                            else if(!pageFound && node_rid.pageNum != ixFileHandle.ixAppendPageCounter-1)
                            {
                                if(ixFileHandle.readPage(ixFileHandle.ixAppendPageCounter-1, pageData2) == -1)
                                    return -1;

                                memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                if(node2_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                {
                                    pageFound = true;
                                    updated_node_page = ixFileHandle.ixAppendPageCounter-1;
                                }
                            }
                            //scan for free space in all pages
                            else if(!pageFound)
                            {
                                for(int i = 0; i < ixFileHandle.ixAppendPageCounter-2 && !pageFound; i++)
                                {
                                    if(ixFileHandle.readPage(i, pageData2) == -1)
                                        return -1;

                                    memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                    memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                    if(node2_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                    {
                                        pageFound = true;
                                        updated_node_page = i;
                                    }
                                }
                            }

                            newBeginOffset = totalPageOffset_2;
                            //if we found a page to hold the data
                            if(pageFound && updated_node_page == node_rid.pageNum)
                            {
                                //update offsets
                                newBeginOffset = totalPageOffset_1;
                                totalPageOffset_1 += node2_offset;
                                totalRIDOffset_1 -= 8;
                                //first update new data to be put into page
                                memcpy((char*)pageData+4092, &totalPageOffset_1 ,sizeof(int));
                                memcpy((char*)pageData+4088, &totalRIDOffset_1 ,sizeof(int));

                                node2_rid.pageNum = node_rid.pageNum;
                                node2_rid.slotNum = totalRIDOffset_1;
                            }
                            else if(pageFound)
                            {
                                //update offsets
                                totalPageOffset_2 += node2_offset;
                                totalRIDOffset_2 -= 8;
                                //first update new data to be put into the second page
                                memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                node2_rid.pageNum = updated_node_page;
                                node2_rid.slotNum = totalRIDOffset_2;
                            }
                            //else we need to append a page
                            else
                            {
                                //update offsets
                                newBeginOffset = 0;
                                totalPageOffset_2 = node2_offset;
                                totalRIDOffset_2 = 4080;
                                //first update new data to be put into the second page
                                memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                node2_rid.pageNum = ixFileHandle.ixAppendPageCounter;
                                node2_rid.slotNum = totalRIDOffset_2;
                            }

                            //we want use pageData
                            if(updated_node_page == node_rid.pageNum)
                            {
                                //add new record of second page
                                memcpy((char*)pageData+node2_rid.slotNum, &newBeginOffset ,sizeof(int));
                                memcpy((char*)pageData+node2_rid.slotNum+4, &totalPageOffset_1 ,sizeof(int));

                                //copy "page" from old page to new page (will add the new value later below)
                                memcpy((char*)pageData+newBeginOffset, (char*)node2, node2_offset);

                                //write(pageData)
                                if(ixFileHandle.writePage(node2_rid.pageNum, pageData) == -1)
                                    return -1;
                            }
                            //else we want to use pageData2
                            else
                            {
                                //add new record of second page
                                memcpy((char*)pageData2+totalRIDOffset_2, &newBeginOffset ,sizeof(int));
                                memcpy((char*)pageData2+totalRIDOffset_2+4, &totalPageOffset_2 ,sizeof(int));

                                //copy "page" from old page to new page (will add the new value later below)
                                memcpy((char*)pageData2+newBeginOffset, (char*)node2, node2_offset);

                                //write(pageData)
                                if(ixFileHandle.writePage(pageData_pageNum, pageData) == -1)
                                    return -1;

                                //write(pageData2)
                                if(pageFound)
                                {
                                    if (ixFileHandle.writePage(node2_rid.pageNum, pageData2) == -1)
                                        return -1;
                                }
                                //else append(pageData2)
                                else
                                {
                                    if (ixFileHandle.appendPage(pageData2) == -1)
                                        return -1;
                                }
                            }

                            //check root case!
                            //if we are we need to create a whole new node and find a place to store it
                            if(is_root)
                            {
                                //we have no more nodes to traverse up so we set this to false
                                need_to_split_node = false;

                                //we can resuse the memory for the first node
                                memcpy((char*)node+4, &node2_active_keys, sizeof(int));
                                memcpy((char*)node+8, &node_rid.pageNum, sizeof(int));
                                memcpy((char*)node+12, &node_rid.slotNum, sizeof(int));
                                switch(selection)
                                {
                                    case 0:
                                        memcpy((char*)node+16, &int_key, sizeof(int));
                                        memcpy((char*)node+20, &node2_rid.pageNum, sizeof(int));
                                        memcpy((char*)node+24, &node2_rid.slotNum, sizeof(int));
                                        node_offset = 24;
                                        break;
                                    case 1:
                                        memcpy((char*)node+16, &real_key, sizeof(float));
                                        memcpy((char*)node+20, &node2_rid.pageNum, sizeof(int));
                                        memcpy((char*)node+24, &node2_rid.slotNum, sizeof(int));
                                        node_offset = 24;
                                        break;
                                    case 2:
                                        memcpy((char*)node+16, &int_key, sizeof(int));
                                        memcpy((char*)node+20, str_key, int_key);
                                        memcpy((char*)node+20+int_key, &node2_rid.pageNum, sizeof(int));
                                        memcpy((char*)node+24+int_key, &node2_rid.slotNum, sizeof(int));
                                        node_offset = 24 + int_key;
                                        break;
                                }

                                //now we check about where to save node2
                                pageFound = false;
                                updated_node_page = -1;
                                pageData_pageNum = node_rid.pageNum;

                                //check current page
                                if(node_offset <= totalRIDOffset_1-totalPageOffset_1)
                                {
                                    pageFound = true;
                                    updated_node_page = node_rid.pageNum;
                                }
                                //check last page for space before scanning
                                else if(!pageFound && node_rid.pageNum != ixFileHandle.ixAppendPageCounter-1)
                                {
                                    if(ixFileHandle.readPage(ixFileHandle.ixAppendPageCounter-1, pageData2) == -1)
                                        return -1;

                                    memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                    memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                    if(node_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                    {
                                        pageFound = true;
                                        updated_node_page = ixFileHandle.ixAppendPageCounter-1;
                                    }
                                }
                                //scan for free space in all pages
                                else if(!pageFound)
                                {
                                    for(int i = 0; i < ixFileHandle.ixAppendPageCounter-2 && !pageFound; i++)
                                    {
                                        if(ixFileHandle.readPage(i, pageData2) == -1)
                                            return -1;

                                        memcpy(&totalPageOffset_2, (char*)pageData2+4092, sizeof(int));
                                        memcpy(&totalRIDOffset_2, (char*)pageData2+4088, sizeof(int));

                                        if(node_offset <= totalRIDOffset_2-totalPageOffset_2-8)
                                        {
                                            pageFound = true;
                                            updated_node_page = i;
                                        }
                                    }
                                }

                                newBeginOffset = totalPageOffset_2;
                                //if we found a page to hold the data
                                if(pageFound && updated_node_page == node_rid.pageNum)
                                {
                                    //update offsets
                                    newBeginOffset = totalPageOffset_1;
                                    totalPageOffset_1 += node_offset;
                                    totalRIDOffset_1 -= 8;
                                    //first update new data to be put into page
                                    memcpy((char*)pageData+4092, &totalPageOffset_1 ,sizeof(int));
                                    memcpy((char*)pageData+4088, &totalRIDOffset_1 ,sizeof(int));

                                    root.pageNum = node_rid.pageNum;
                                    root.slotNum = totalRIDOffset_1;
                                }
                                else if(pageFound)
                                {
                                    //update offsets
                                    totalPageOffset_2 += node_offset;
                                    totalRIDOffset_2 -= 8;
                                    //first update new data to be put into the second page
                                    memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                    memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                    root.pageNum = updated_node_page;
                                    root.slotNum = totalRIDOffset_2;
                                }
                                    //else we need to append a page
                                else
                                {
                                    //update offsets
                                    newBeginOffset = 0;
                                    totalPageOffset_2 = node_offset;
                                    totalRIDOffset_2 = 4080;
                                    //first update new data to be put into the second page
                                    memcpy((char*)pageData2+4092, &totalPageOffset_2 ,sizeof(int));
                                    memcpy((char*)pageData2+4088, &totalRIDOffset_2 ,sizeof(int));

                                    root.pageNum = ixFileHandle.ixAppendPageCounter;
                                    root.slotNum = totalRIDOffset_2;
                                }

                                //we want use pageData
                                if(updated_node_page == node_rid.pageNum)
                                {
                                    //add new record of second page
                                    memcpy((char*)pageData+root.slotNum, &newBeginOffset ,sizeof(int));
                                    memcpy((char*)pageData+root.slotNum+4, &totalPageOffset_1 ,sizeof(int));

                                    //copy "page" from old page to new page (will add the new value later below)
                                    memcpy((char*)pageData+newBeginOffset, (char*)node, node_offset);

                                    //write(pageData)
                                    if(ixFileHandle.writePage(root.pageNum, pageData) == -1)
                                        return -1;
                                }
                                //else we want to use pageData2
                                else
                                {
                                    //add new record of second page
                                    memcpy((char*)pageData2+totalRIDOffset_2, &newBeginOffset ,sizeof(int));
                                    memcpy((char*)pageData2+totalRIDOffset_2+4, &totalPageOffset_2 ,sizeof(int));

                                    //copy "page" from old page to new page (will add the new value later below)
                                    memcpy((char*)pageData2+newBeginOffset, (char*)node, node_offset);

                                    //write(pageData2)
                                    if(pageFound)
                                    {
                                        if (ixFileHandle.writePage(root.pageNum, pageData2) == -1)
                                            return -1;
                                    }
                                        //else append(pageData2)
                                    else
                                    {
                                        if (ixFileHandle.appendPage(pageData2) == -1)
                                            return -1;
                                    }
                                }
                            }
                            //else load moveup values into current_rid, key_holders, split2_rid (and remove from node)
                            else {
                                current_rid = node_rid;
                                split2_rid = node2_rid;

                                //update the node_rid value
                                current_rid = node_rid;
                                is_tombstone = true;
                                do {
                                    node_rid = path_to_leaf.back();
                                    is_tombstone = path_is_tombstone.back();
                                    path_to_leaf.pop_back();
                                    path_is_tombstone.pop_back();
                                    if (is_tombstone)
                                        selected_tomb_rid = node_rid;
                                } while (is_tombstone && !is_root);
                            }
                        }

                    }while(need_to_split_node);

                    free(split1);
                    free(split2);
                    free(node);

//                    free(pageData);
                    free(pageData2);
                    free(node2);
                }


                //clear path_to_leaf if IMPORTANT
                path_to_leaf.clear();
            }
        }


        free(pageData);
        return 0;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    //=================================================================================================================

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    //=================================================================================================================

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;

//        totalPages = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC IXFileHandle::setFileName(const std::string &fileName)
    {
        handle.file = fileName.c_str();
    }

    RC IXFileHandle::readPage(PageNum pageNum, void *data)
    {
        if(handle.readPage(pageNum, data) == -1)
            return -1;

        ++ixReadPageCounter;
        return 0;
    }

    RC IXFileHandle::writePage(PageNum pageNum, const void *data)
    {
        if(handle.writePage(pageNum, data) == -1)
            return -1;

        ++ixWritePageCounter;
        return 0;
    }

    RC IXFileHandle::appendPage(const void *data)
    {
        if(handle.appendPage(data) == -1)
            return -1;

        ++ixAppendPageCounter;
        return 0;
    }

    //dont think I will need this but we'll see
    unsigned IXFileHandle::getNumberOfPages()
    {
        return-1;
    }


    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;

        return 0;
    }

} // namespace PeterDB