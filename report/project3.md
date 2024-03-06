## Project 3 Report

### 1. Basic information
 - Team #: N/A
 - Github Repo Link: https://github.com/mazorith/CS222-DB
 - Student 1 UCI NetID: 85845302
 - Student 1 Name: Ian Harshbarger


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

I do have a meta-data page but it is still the same from by pfm. I haven't really had a need for other metadata. 


### 3. Index Entry Format
- Show your index entry design (structure).

  - entries on internal nodes:

My Indexes in leaf nodes are stored between page numbers values. The page number value to the left of the index indicates
that everything from that pointer on will be less than the index value and the page number value on the right of the index 
will indicate that everything from that pointer on will be greater or equal to the index value.

  - entries on leaf nodes:

My indexes in internal nodes are stored right before the RID value pair. I do allow for duplicates to be stored in the nodes, rather than
trying to append more RID values. This is possible however it would take more time then I have to implement.


### 4. Page Format

For all nodes (after my first attempt, explained in part 8) Each node will take up a single page. It's a bit wasteful
but that was the only way to get it to pass tests.

Each node has a single 4-byte integer at the begining of the file to indicate if it is a leaf node or not. This is followed by
x number (4 in my implementation) additional 4-byte integers to denote the offset of the keys in the node. If the offset
is -1 then we know that slot is empty in the node.

- Show your internal-page (non-leaf node) design.

In the internal node we can have a page number value stored to the left or right of the index key value. These are also 
4-byte integers. As long as we know the byte-length of the index we can always quickly find any page number values related to it

No other information needs to be stored.

- Show your leaf-page (leaf node) design.
  
For the non-leaf node page the design is even simpler. Since from the first few bytes of the page we know where each element is,
as long as we know the length of the index key we can easily find the RID associated with it. I also place an page number pointer
at the end of the offset values at the begining of the page which points to the next leaf-node in ascending key order.

No other information needs to be stored.

### 5. Describe the following operation logic.
- Split

While I didnt get to fully implement this (see part 8). My design and partial implementation for it is as follows:

0.) as we are searching for where to insert a value we save the "path"  we are taking to get there as a sequence of page numbers

1.) allocated extra three pages of memory initially.

2.) First split the leaf node in two and load each half into extra allocated memory slots.

3.) Re-save the first split into the orginal leaf-node page and append the new leaf node split we created.

3.1) link the first split page num slot to the second split's new appended page number.

4.) Now we save both page numbers at of the first and second splits as well as the index key values at the beginning of the second split page.

5.) We slot these values (preserving keys in ascending order) into the most recent internal-node

6.) If the most recent internal-node becomes full we follow the same splitting process as many times as needed.

7.) If we end up spliting on the root node, we will then save the index key value and its two page number pointers as a(the) new internal root-node.

- Rotation (if applicable)

N/A

- Merge/non-lazy deletion (if applicable)

N/A

- Duplicate key span in a page

At the current time, duplicate keys don't hurt my design, they just end up consuming more memory and pages. It is a trade-off I was willing to accept
in order to get as much of this assignment done as I could.

- Duplicate key span multiple pages (if applicable)

At the current time, duplicate keys don't hurt my design. Everything will still function optimally we will just have a little extra bloat.

### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.

Nope, all my code edits are purely my own creation, stuffed in the original files.

- Other implementation details:

N/A

### 7. Member contribution (for team of two)
- Explain how you distribute the workload in team.

N/A

### 8. Other (optional)
- Freely use this section to tell us about things that are related to the project 3, but not related to the other sections (optional)

So I assumed we would be able to implement our b+tree however we wanted, until I spent a whole week working on my implementation only to figure out
that the tests want:

1.) A root node created at the first insert
2.) Each node housed on a separate page

I figured the implementation of the b+tree would be the most critical part as the other functions are easier compared to its implementation.
(which turned out to be true thankfully as I was able to get my code to pass the first 3-5 testcases in a couple of hours to save some points)
I'm currently writing this as I had this realization. I will do my best to get the as many tests work as possible now before the deadline...

If you are interested in my implementation please do look at the ix-old.cc file. Its mostly just the b+tree so it can fit more dynamically in
a file and take up minial space (though I am leaving tombstones in someplaces)


- Feedback on the project to help improve the project. (optional)
