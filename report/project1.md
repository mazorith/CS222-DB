## Project 1 Report


### 1. Basic information
 - Team #: ---
 - Github Repo Link: https://github.com/mazorith/CS222-DB
 - Student 1 UCI NetID: 85845302
 - Student 1 Name: Ian Harshbarger


### 2. Internal Record Format
- Show your record format design.

My record formant design is fairly simple. On attempting to insert the first record I create a page. If we run out of space on 
said page, or at a later time all pages, we create another new page. In all other cases we can manipulate the existing pages provided
we keep track of the record ids. Additionally for debugging purposes, I currently remove tombstones in my design as the appear. I have
been having memory errors which prevent me from leaving in tombstones. Until it is optimal to compress/remove them I am making this design choice.

- Describe how you store a null field.

To store a null field, I took the ceiling of the amount of elements in the given records vector over the size of a char (8bits) and 
used that to define the number of elements with the null field. With that information is just a matter of ensuring that I reserved 
enough char sized slots of data at the beginning of each record for the field.

- Describe how you store a VarChar field.

To store the VarChar field I use the first 4 bytes of the field as an integer which reveals the length of varchar field in bytes. 
After that I just copy the bytes of that length to the record for storing.

- Describe how your record design satisfies O(1) field access.

Given the record id which stores the page and slot-id of the slot directory, we can directly look up a record with those values 
without having to iterate through the existing records/pages to find it. -> Select page, Select slot-id, read offset, read record

### 3. Page Format
- Show your page format design.

My page format design is similar to that discussed in class with records, a freespace value, a pointer to latest value of the slot directory,
and said slot directory. I did not have the time to fully implement all functions so currently the page format is tombstone free. All records
are stored at the top/begining of the page while a max record index and max slot-id index are stored at the very last 8 bytes of page. Additionaly
the slot directory will grow from bottom to top with added records. If any tombstones are generated (for both records or slot-ids) then we 
consolidate the freespace and condense all records towards the top of the page and all slots-ids towards the bottom.

- Explain your slot directory design if applicable.



### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

Due to my current tombstone free design, this is a single O(n) check where n is the number of pages. Since I do not have tombstones
I can take the difference between the max slot-id index and the max record index. This difference is the free space. If the free space is
greater than the record size and 12 bytes for the slot-id element then we have enough space to place the record in the page.

- How many hidden pages are utilized in your design?

I utilize a single hidden page to keep track of a files total pages, read, write, and append values. This is to allow for a uniform design of
data pages without the need to code special cases of pages.

- Show your hidden page(s) format design if applicable

It is a single hidden page where the first 16 bytes are reserved for the values mentioned above.

### 5. Implementation Detail
- Other implementation details goes here.



### 6. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 7. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)
- Feedback on the project to help improve the project. (optional)

For the past 4-5 days My machine has been constantly having memory problems which is has caused working code to fail at 
either random times or when first created. I am still having these problems, I'm not sure what entirely to do about it at the moment.
It seems to only be prevent when using large amounts of data/memory