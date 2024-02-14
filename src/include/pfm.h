#ifndef _pfm_h_
#define _pfm_h_

#define PAGE_SIZE 4096

#include <string>
#include <fstream>
#include <cstring>
#include <vector>

namespace PeterDB {

    typedef unsigned PageNum;
    typedef int RC;

    class FileHandle;

    class PagedFileManager {
    public:
        static PagedFileManager &instance();                                // Access to the singleton instance

        static RC createFile(const std::string &fileName);                         // Create a new file
        static RC destroyFile(const std::string &fileName);                        // Destroy a file
        static RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a file
        static RC closeFile(FileHandle &fileHandle);                               // Close a file

    protected:
        PagedFileManager();                                                 // Prevent construction
        ~PagedFileManager();                                                // Prevent unwanted destruction
        PagedFileManager(const PagedFileManager &);                         // Prevent construction by copying
        PagedFileManager &operator=(const PagedFileManager &);              // Prevent assignment

    };

    class FileHandle {
    public:
        // variables to keep the counter for each operation
        //these will be saved on the very final page. The very final page will only contain these values
        unsigned readPageCounter = 0;
        unsigned writePageCounter = 0;
        unsigned appendPageCounter = 0;
        bool has_saved_values = false;
        //will save attributes on final hidden page(s)
        //first byte will denote the amount of attribute elements
        //subsequent bytes will denote the data type. (0,1, or 2)
        unsigned masterAttributeCount = 0;
        unsigned attributePageCounter = 0; //number of pages after appended data pages
        std::vector<std::vector<unsigned>> attributes;
        //the names for each attribute will be saved
        std::vector<std::vector<std::string>> attribute_names;

        const char *file;
        const char *counterFile;
        unsigned totalPages = 0;

        FileHandle();                                                       // Default constructor
        ~FileHandle();                                                      // Destructor

        RC readPage(PageNum pageNum, void *data);                           // Get a specific page
        RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
        RC appendPage(const void *data);                                    // Append a specific page
        unsigned getNumberOfPages();                                        // Get the number of pages in the file
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount,
                                unsigned &appendPageCount);                 // Put current counter values into variables
    };

} // namespace PeterDB

#endif // _pfm_h_