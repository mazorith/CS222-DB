## Project 2 Report


### 1. Basic information
 - Team #: N/A
 - Github Repo Link: https://github.com/mazorith/CS222-DB
 - Student 1 UCI NetID: 85845302
 - Student 1 Name: Ian Harshbarger

### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.

I used what was described in the project 2 page on canvas. I did not have time to attempt an implmentation of a
more robust design for features of adding new attributes to tables/columns, nor did I need any additonal attributes
to complete the non-extra-credit tests.

### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

Same a P1


- Describe how you store a null field.

Same as P1


- Describe how you store a VarChar field.

Same as P1


- Describe how your record design satisfies O(1) field access.

Same as P1


### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

Though you can see in my code I experimented with this I did not actually implement it in the project. Thus same as P1.


- Explain your slot directory design if applicable.

Same as P1


### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

Same as P1

- Show your hidden page(s) format design if applicable

Same as P1

### 6. Describe the following operation logic.

- Delete a record

To delete a record I set the record ID slot values to -1, for later identification of an, empty slot. I then compact the data.
If the record is the last stored in the top of the page I simply adjust the total end value and ignore actually overwriting the
data, since logically it now isn't there. If the record is in the begining or middle of the page then I take the chunck of memory 
after the record to be deleted and shift it left by the size of the record to be deleted.

For the record aspect this is handled in the RBFM. The implmentation in th RM is just call calling the getAttributes function 
for the table (with validation checks of course) and then calling the rbfm functions.

- Update a record

To handle the compacting by calling the delete record function. I then check the current page to see if there is free space. If so,
I add the updated record as if inserting a new record (also searching for free record IDs). If there is no free space I search all pages,
first the most recent page, for free space or if I need to append a page. Wherever the new record ends up I then save 8 bytes of data
to the updated records existing page which act as the rid values to the new page where the updated record is located.

As above, for the record aspect this is handled in the RBFM. The implmentation in th RM is just call calling the getAttributes function
for the table (with validation checks of course) and then calling the rbfm functions.

- Scan on normal records

This was a bit of challenge as the I was having some, currently, unexplainable memory errors when using the rbfm_scanIterator.
The memory and variables in my rbfm_scanIterator would constantly change values and memory address and each instruction call. I
don'y know why so my current workaround is to actually prescan the whole file and save a vector of rids and return it via the 
rbfm_scanIterator to the rm_scanIterator. Then simply using a custom function I made for reading extracting only the 
specified feilds with the rbfm can I can just look at the next rid in the vector and return it data. 

To my understanding I am not saving the data in memory and the rbfm is still handeling all reading and writting to the tables
file. Thus, I figured this workaround would be acceptable for the time being. It just has a small memory cost (the vector can be
saved to the disk if nessicary) and front loads all scan to be in one instance function call instead of through the iterator, since
it wont work.

- Scan on deleted records

This follows the same way we fail a read, update, or delete on  deleted records in the rbfm functions. We just instead will skip over 
the value instead of returning the value of -1 indicating failure for reading a deleted file.

- Scan on updated records

If we look at the total record size and see it is only 8 bytes we will know it is rid link to another page and can act like we do in the 
readRecord function of the RBFM. (since the function handling this is in the RBFM anyway.) But to make this more robust, since we can 
store newer values in earlier pages on an update. We also need some way to ignore the rids which are being pointed to by a tombstone. 
To work around this I add an additional field to rid section of the dataPage. If it is 0 it is a pointer to another page, -1 if it is
a deleted value, and -2 it that record is being pointed two by a tombstone. We can just check this value during a scan and chose
to ignore it if its -1 or -2, else we can just use the saved rid in the tombstone to quickly check the updated value.

### 7. Implementation Detail
- Other implementation details goes here.

So from project 1 a actually updated a couple of things. Firstly I realized that I misunderstood the definition of tombstones.
When updating a record I do have tombstones which point to the another record in another page if space demands but I COMPACT the page
leaving no holes. Secondly I updated my insert function to check the most recent appended page for free space before scanning all
of the previous pages for free space. This has sped up my insert tests.

### 8. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A


### 9. Other (optional)
- Freely use this section to tell us about things that are related to the project 2, but not related to the other sections (optional)

- Feedback on the project to help improve the project. (optional)

I was struggling to complete this on time as I had a funeral to attend for my grandmother. I realized that the later projects in this course
may be more doable if a solution of the previous project is give for the next assignment. I do not fully know the logistics of what that would
take but I know these assignments are taking far longer than I can find time for and trying to work on the next stages seems to require the
previous ones.