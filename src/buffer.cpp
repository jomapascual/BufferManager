/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 *
 * 		Names: 				Student IDs:
 * 	   	- Jose Pascual			- 9078122448
 *		- Samuel Ong			- 9078422244
 *		- Samuel Gronseth		- 9077184696
 *		- Burak Marmara			- 9075650144
 *
 * This file contains the implementation of the Buffer Manager for the BadgerDB. The buffer
 manager  is used to control which pages to keep in memory. Whenever the buffer manager receives a
 request for a data page, it checks to see if the requested page is already in the one of the frames that constitute the buffer pool. If so, the buffer manager simply returns a pointer to the page. If not, the buffer manager frees a frame(possibly writing the page it contains to disk if the page is dirty) and then reads in the requested page from disk into the frame that has been freed.

This file contains different methods to perform buffer manager functions described as follows:
 	- A constructor that creates an instance of the BufMgr class with a given size. 
	- A method to flush out dirty pages and deallocate the buffer pool and tables. 
	- A method to advance the clock hand used in the implementation of the clock algorithm.
	- A method to allocate a buffer frame
	- A method to read in a page into a file
	- A method to unpin a page of a file
	- A method to allocate a page into a file
	- A method to delete a page from a file
	- A method to flush a file and its contents
 *
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 * Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
 * @param none
 * @return void
 */
BufMgr::~BufMgr() {
	for (FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].dirty == true) { // If dirtybit == true, flush the page
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufDescTable; // Deallocation
	delete [] bufPool;
	delete hashTable;
}

/**
 * Advances clock to the next frame in the buffer pool.
 * @param none
 * @return void
 */
void BufMgr::advanceClock()
{
	if(this->clockHand == this->numBufs - 1) {
		this->clockHand = 0;
	}
	else {
		this->clockHand++;
	}
}

/**
 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back
	to disk. Throws BufferExceededException if all buffer frames are pinned. This private
	method will get called by the readPage() and allocPage() methods described below. Make
	sure that if the buffer frame allocated has a valid page in it, you remove the appropriate
	entry from the hash table.
 * @param frame pointer to newly allocated frame
 * @return pointer to the newly allocated frame
 * @throws BufferExceededException, if all buffer frames are pinned
 */
void BufMgr::allocBuf(FrameId & frame) 
{
	std::uint32_t pincount = 0;

	while(pincount <= numBufs - 1) 
	{

		this->advanceClock(); //Advance Clock Pointer
		frame = clockHand; // set frame to current clockhand

		if (bufDescTable[frame].valid == false) 
		{
			return;
		} else {

			if (bufDescTable[frame].refbit == true) 
			{ //Checks if refbit has been set
				bufDescTable[frame].refbit = false;
				continue;
			} else {

				if(bufDescTable[frame].pinCnt > 0) 
				{ //Checks if Page has been pinned
					pincount++;
					continue;
				}
				else if(bufDescTable[frame].dirty == true) 
				{ //Checks Dirty Bit
					bufDescTable[frame].dirty = false;
					// write page back to disk
					bufDescTable[frame].file->writePage(bufPool[frame]);
				}
				try{ //remove entry from hashtable
					hashTable->remove(bufDescTable[frame].file, bufDescTable[frame].pageNo);
				} catch(HashNotFoundException e){

				}
				return;
			}
		}
	}

	if(pincount > numBufs - 1) 
	{ // exceeds buffer size or frames are all pinned
		throw BufferExceededException();
	}

	bufDescTable[frame].Clear();

	// return pointer to newly alloc buffer frame via frame param
	frame = bufDescTable[clockHand].frameNo;
}

/**
 * First check whether the page is already in the buffer pool by invoking the lookup() method,
	which may throw HashNotFoundException when page is not in the buffer pool, on the
	hashtable to get a frame number. There are two cases to be handled depending on the
	outcome of the lookup() call:
 		Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and
			then call the method file->readPage() to read the page from disk into the buffer pool
			frame. Next, insert the page into the hashtable. Finally, invoke Set() on the frame to
			set it up properly. Set() will leave the pinCnt for the page set to 1. Return a pointer
			to the frame containing the page via the page parameter.
 		Case 2: Page is in the buffer pool. In this case set the appropriate refbit, increment
			the pinCnt for the page, and then return a pointer to the frame containing the page
			via the page parameter.
 * @param file the File instance to contain the page to read
 * @param pageNo the page number of the page to read
 * @param page pointer to the frame containing the newly read page
 * @return pointer to the frame containing the page
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = 0; // to hold the frame num returned by lookup()

	try {

		hashTable->lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].refbit = true; // set reference bit to true
		bufDescTable[frameNo].pinCnt++; // increment pin count
		page = &bufPool[frameNo]; //return pointer to frame containing page

	} catch (HashNotFoundException noHash) {

		try {

			allocBuf(frameNo); // alloc a buffer frame for page
			bufPool[frameNo] = file->readPage(pageNo); // read page into buffer
			hashTable->insert(file, pageNo, frameNo); // update hashtable
			bufDescTable[frameNo].Set(file, pageNo); // set up frame properly
			page = &bufPool[frameNo];

		} catch(BufferExceededException) {
			// do nothing for exceptions
		}		
	} catch(...) {
		// Dont do anything
	}
}

/**
 * Decrements the pinCnt of the frame containing (file, PageNo) and, 
 if dirty == true, sets the dirty bit. 
 Throws PAGENOTPINNED if the pin count is already 0. 
 Does nothing if page is not found in the hash table lookup.
 * @param file the File instance ocntaining the page to unpin
 * @param pageNo the page number of the page to unpin
 * @param dirty the bit representing whether the page was modified
 * @return void
 * @throws PageNotPinnedException, if the pin count of a page is already 0
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try{

		hashTable->lookup(file, pageNo, frameNo); // look up page

		if (dirty) 
		{ // If dirty == true, sets the dirty bit
			bufDescTable[frameNo].dirty = true;
		}

		if (bufDescTable[frameNo].pinCnt == 0) 
		{ // Pincount is already 0
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}

		bufDescTable[frameNo].pinCnt--; // Decrements the pincount

	}
	catch (HashNotFoundException &e) {
		// Do nothing for exceptions
	}
	
}

/**
 * The first step in this method is to to allocate an empty page in the specified file by invoking
	the file->allocatePage() method. This method will return a newly allocated page.
	Then allocBuf() is called to obtain a buffer pool frame. Next, an entry is inserted into the
	hash table and Set() is invoked on the frame to set it up properly. The method returns
	both the page number of the newly allocated page to the caller via the pageNo parameter
	and a pointer to the buffer frame allocated for the page via the page parameter.
 * @param file the File instance representing the file where where the page will be allocated
 * @param pageNo the page number of the newly allocated page
 * @param page pointer to the newly allocated page
 * @return page number of the newly allocated page, pointer to buffer frame for newly allocated page
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	FrameId frameNo = 0;

	// alloc empty page in the specified file 
	Page newPage = file->allocatePage();
	
	// call allocBuf() to obtain buffer pool frame
	allocBuf(frameNo);

	// after bufPool frame obtained, set page to bufPool frame from allocBuf
	bufPool[frameNo] = newPage;

	// insert into hashtable
	hashTable->insert(file, newPage.page_number(), frameNo);

	// call Set() to set frame properly
	bufDescTable[frameNo].Set(file, newPage.page_number());

	pageNo = newPage.page_number();
	page = &bufPool[frameNo];

}

/**
 * Should scan bufTable for pages belonging to the file. For each page encountered it should:
	(a) if the page is dirty, call file->writePage() to flush the page to disk and then set the dirty bit for the page to false, (b) remove the page from the hashtable (whether the page is clean or dirty) and (c) invoke the Clear() method of BufDesc for the page frame.
	Throws PagePinnedException if some page of the file is pinned. Throws BadBuffer-
	Exception if an invalid page belonging to the file is encountered.
 * @param file the File object instance to flush 
 * @return void
 * @throws PagePinnedException, if a page of the file is pinned
 * @throws BadBufferException, if an invalid page belonging to the file is encountered
 */
void BufMgr::flushFile(const File* file) 
{
	// scan/iterate through pages in bufTable belonging to file
	for(std::uint32_t i = 0; i < numBufs; i++)
	{
		// page belongs to file and is valid
		if(bufDescTable[i].file == file && bufDescTable[i].valid)
		{
			// if page dity, call writePage() to flush to disk & set dirty bit to false
			if(bufDescTable[i].dirty)
			{
				bufDescTable[i].file->writePage(bufPool[bufDescTable[i].frameNo]);
				bufDescTable[i].dirty = false;
			}

			// if page of file is pinned, throw exception
			if(bufDescTable[i].pinCnt != 0)
			{
				throw PagePinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}

			// remove page from hashtable
			hashTable->remove(file, bufDescTable[i].pageNo);
			// invoke clear() method for page frame
			bufDescTable[i].Clear();
		}

		//page belongs to file but it is not valid, throw exception
		if(bufDescTable[i].file == file && bufDescTable[i].valid == false)
		{
			throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
		}
	}
}

/**
 * This method deletes a particular page from file. Before deleting the page from file, it
 	makes sure that if the page to be deleted is allocated a frame in the buffer pool, that frame
	is freed and correspondingly entry from hash table is also removed.
 * @param file the File object instance where the page will be disposed from
 * @param PageNo the page number of the page to dispose of in the file
 * @return void
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNum;
	try {

		// make sure page to be deleted is allocated in buffer pool
		hashTable->lookup(file, PageNo, frameNum);
		// clear entry from description table for the frame
		bufDescTable[frameNum].Clear();
		// remove correspoding entry from hashtable
		hashTable->remove(file, PageNo);

	} catch (HashNotFoundException e) {
		// this means page to be removed was never alloc as a frame in buffer pool
	}
	// delete page from file
	file->deletePage(PageNo);
}

/**
 * Print member variable values. 
 * @param void 
 * @return void
 */
void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
