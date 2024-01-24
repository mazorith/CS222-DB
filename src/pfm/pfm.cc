#include "src/include/pfm.h"

#include <iostream>

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
        if(std::fopen(fileName.c_str(), "r") != NULL)
        {
            fileHandle.file = fileName.c_str();

            FILE * info = std::fopen(fileHandle.file, "r");

            fseek(info, 0, SEEK_END);
            if (ftell(info) >= (1) *4096)
            {
                fseek(info, -4096, SEEK_END);
                char value;

                fread(&value, sizeof(char), 1, info);
                fileHandle.totalPages = value;

                fread(&value, sizeof(char), 1, info);
                fileHandle.appendPageCounter = value;

                fread(&value, sizeof(char), 1, info);
                fileHandle.writePageCounter = value;

                fread(&value, sizeof(char), 1, info);
                fileHandle.readPageCounter = value;



                fileHandle.has_saved_values = true;
            }

            fclose(info);
        }
        else
            return -1;

        return 0;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        //TODO: the flush

        //FILE * info = std::fopen(fileHandle.file, "r+");


        //unsigned values[4] = {fileHandle.appendPageCounter, fileHandle.writePageCounter, fileHandle.readPageCounter, fileHandle.totalPages};
//        char padding[4096];
//        int values[1] = {fileHandle.totalPages};

//        fseek(info, 4906, SEEK_END);
//        fwrite(padding, 4096, 1, info);

        if (fileHandle.has_saved_values == true)
        {
            FILE * info = std::fopen(fileHandle.file, "r+");

            fseek(info, -4096, SEEK_END);

            fwrite(&fileHandle.totalPages, sizeof(char), 1, info);
            fwrite(&fileHandle.appendPageCounter, sizeof(char), 1, info);
            fwrite(&fileHandle.writePageCounter, sizeof(char), 1, info);
            fwrite(&fileHandle.readPageCounter, sizeof(char), 1, info);

            fclose(info);
        }
        else
        {
            FILE * info = std::fopen(fileHandle.file, "a");

            fwrite(&fileHandle.totalPages, sizeof(char), 1, info);
            fwrite(&fileHandle.appendPageCounter, sizeof(char), 1, info);
            fwrite(&fileHandle.writePageCounter, sizeof(char), 1, info);
            fwrite(&fileHandle.readPageCounter, sizeof(char), 1, info);

            for(int i = 0; i < 4092; i++) //1023 = 4096
                fwrite(&fileHandle.totalPages, sizeof(char), 1, info);

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