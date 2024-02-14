#include "src/include/pfm.h"

#include <iostream>
#include <cmath>

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {
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

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if(std::fopen(fileName.c_str(), "r") != NULL)
            std::remove(fileName.c_str());
        else
            return -1;

        return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
//        FILE * info = std::fopen(fileHandle.file, "r");
        if(FILE * info = std::fopen(fileName.c_str(), "r"))
        {
            fileHandle.file = fileName.c_str();

//            FILE * info = std::fopen(fileHandle.file, "r");

            fseek(info, 0, SEEK_END);
            if (ftell(info) >= (1) *4096)
            {
                fseek(info, -4096, SEEK_END);
                unsigned value;
//                char v2;

                fread((char*)&value, sizeof(unsigned), 1, info);
                fileHandle.totalPages = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                fileHandle.appendPageCounter = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                fileHandle.writePageCounter = value;

                fread((char*)&value, sizeof(unsigned), 1, info);
                fileHandle.readPageCounter = value;

//                fread((char*)&v2, sizeof(char), 1, info);
//                fileHandle.masterAttributeCount = v2;
//
//                fread((char*)&value, sizeof(char), 1, info);
//                fileHandle.attributePageCounter = v2;

                //if file contains attribute tables (if master count > 0 then saved page count > 0)
//                if(fileHandle.masterAttributeCount > 0)
//                {
//                    fseek(info, -4096*fileHandle.attributePageCounter, SEEK_END);
//                    unsigned num_attr = 0;
//                    for(int i = 0; i < fileHandle.masterAttributeCount; i++)
//                    {
//                        fread(&value, sizeof(char), 1, info);
//                        num_attr = value;
//
//                        //vectors to save locally for reference
//                        std::vector<unsigned> attr_vec;
//                        std::vector<std::string> name_vec;
//
//                        //load attribute information
//                        attr_vec.push_back(num_attr);
//                        for(int j = 0; j < num_attr; j++)
//                        {
//                            fread(&value, sizeof(char), 1, info);
//                            attr_vec.push_back(value);
//                        }
//                        fileHandle.attributes.push_back(attr_vec);
//
//                        //load attribute names
//                        unsigned length = 0;
//                        std::string att_name = "";
//                        for(int j = 0; j < num_attr; j++)
//                        {
//                            fread(&length, sizeof(char), 1, info);
//                            fread(&att_name, length, 1, info);
//                            name_vec.push_back(att_name);
//                        }
//                        fileHandle.attribute_names.push_back(name_vec);
//                    }
//                }

                fileHandle.has_saved_values = true;
            }

            fclose(info);
        }
        else
            return -1;

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.has_saved_values == true)
        {
            FILE * info = std::fopen(fileHandle.file, "r+");

//            if(fileHandle.masterAttributeCount > 0)
//            {
//                fseek(info, -4096*fileHandle.attributePageCounter, SEEK_END);
//
//                int bytes_written = 0;
//                for(int i = 0; i < fileHandle.masterAttributeCount; i++)
//                {
//                    for (int j = 0; j < fileHandle.attributes[i].size(); j++)
//                    {
//                        fwrite(&fileHandle.attributes[i][j], sizeof(char), 1, info);
//                        bytes_written++;
//                    }
//                    int size_holder = 0;
//                    for (int j = 0; j < fileHandle.attribute_names[i].size(); j++)
//                    {
//                        size_holder = unsigned(fileHandle.attribute_names[i][j].size());
//                        fwrite(&size_holder, sizeof(char), 1, info);
//                        bytes_written++;
//
//                        fwrite(&fileHandle.attribute_names[i][j], size_holder, 1, info);
//                        bytes_written += size_holder;
//                    }
//                }
//
//                fileHandle.attributePageCounter = ceil(double(bytes_written)/4096.0);
//                for(int i = 0; i < (4096*fileHandle.attributePageCounter)-bytes_written; i++)
//                    fwrite(&fileHandle.totalPages, sizeof(char), 1, info);
//            }

            fseek(info, -4096, SEEK_END);

            fwrite((char*)&fileHandle.totalPages, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.appendPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.writePageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.readPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.masterAttributeCount, sizeof(char), 1, info);
            fwrite((char*)&fileHandle.attributePageCounter, sizeof(char), 1, info);


            fclose(info);
        }
        else
        {
            FILE * info = std::fopen(fileHandle.file, "a");

//            if(fileHandle.masterAttributeCount > 0)
//            {
//                int bytes_written = 0;
//                for(int i = 0; i < fileHandle.masterAttributeCount; i++)
//                {
//                    for (int j = 0; j < fileHandle.attributes[i].size(); j++)
//                    {
//                        fwrite(&fileHandle.attributes[i][j], sizeof(char), 1, info);
//                        bytes_written++;
//                    }
//                    int size_holder = 0;
//                    for (int j = 0; j < fileHandle.attribute_names[i].size(); j++)
//                    {
//                        size_holder = unsigned(fileHandle.attribute_names[i][j].size());
//                        fwrite(&size_holder, sizeof(char), 1, info);
//                        bytes_written++;
//
//                        fwrite(&fileHandle.attribute_names[i][j], size_holder, 1, info);
//                        bytes_written += size_holder;
//                    }
//                }
//
//                fileHandle.attributePageCounter = ceil(double(bytes_written)/4096.0);
//                for(int i = 0; i < (4096*fileHandle.attributePageCounter)-bytes_written; i++)
//                    fwrite(&fileHandle.totalPages, sizeof(char), 1, info);
//            }

            fwrite((char*)&fileHandle.totalPages, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.appendPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.writePageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.readPageCounter, sizeof(unsigned), 1, info);
            fwrite((char*)&fileHandle.masterAttributeCount, sizeof(char), 1, info);
            fwrite((char*)&fileHandle.attributePageCounter, sizeof(char), 1, info);

            for(int i = 0; i < 4078; i++) //1023 = 4096
                fwrite((char*)&fileHandle.totalPages, sizeof(char), 1, info);

            fclose(info);
        }

//        fclose(info);

        return 0;
    }

    FileHandle::FileHandle() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        FILE * aFile = std::fopen(file, "r");

        fseek(aFile, 0, SEEK_END);
        if(ftell(aFile) < (pageNum) *4096)
            return -1;

        if(pageNum > 0)
            fseek(aFile, (pageNum * 4096), SEEK_SET);
        else
            fseek(aFile, 0, SEEK_SET);

        fread(data, 4096, 1, aFile);
//        for(int i = 0; i < 4096; i++)
//            *((char *) data + i) = (char) fgetc(aFile);

//        for(int i = 0; i < 4096; i++)
//        {
//            if(i>4080)
//            std::cout << "read: " << *((char *) data + i);
//        }
//        std::cout << '\n';

        fclose(aFile);
        ++readPageCounter;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        FILE * aFile = std::fopen(file, "r+");

        fseek(aFile, 0, SEEK_END);
        if(ftell(aFile) < (pageNum) *4096)
            return -1;

        if(pageNum > 0)
            fseek(aFile, (pageNum * 4096), SEEK_SET);
        else
            fseek(aFile, 0, SEEK_SET);

//        char buff[4096];
//        for(int i = 0; i < 4096; i++)
//             buff[i] = *((char *) data + i);

        fwrite((char*)data, 4096, 1, aFile);

        fclose(aFile);
        ++writePageCounter;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {

        if (has_saved_values == true)
        {
            has_saved_values = false;
            FILE * aFile = std::fopen(file, "r+");
            fseek(aFile, -4096, SEEK_END);

//            for (int i = 0; i < 4096; i++)
//                fputc(*((char *) data + i), aFile);

            fwrite(data, 4096, 1, aFile);

            fclose(aFile);
        }
        else
        {
            FILE * aFile = std::fopen(file, "a");

            fwrite(data, 4096, 1, aFile);
//            for (int i = 0; i < 4096; i++)
//                fputc(*((char *) data + i), aFile);

            fclose(aFile);
        }

//        for(int i = 0; i < 4096; i++)
//        {
//            if(i>4080)
//                std::cout << "append " << (appendPageCounter + 1) << ": " << *((char *) data + i);
//        }
//        std::cout << '\n';

        ++appendPageCounter;
//        ++writePageCounter;
        ++totalPages;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return totalPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB