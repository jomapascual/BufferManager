/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
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
 */
BufMgr::~BufMgr() {
	//FrameId frames [numBufs]; // Array of FrameId's
	for (FrameId i = 0; i < numBufs; i++) {
		if(bufDescTable[i].dirty == true) { // If dirtybit == true, flush the page
			// bufDescTable[i].dirty = false;
			// bufDescTable[i].file -> writePage(bufPool[i]); // Writes the page
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufDescTable; // Deallocation
	delete [] bufPool;
	delete [] hashTable;
}

void BufMgr::advanceClock()
{
	if(this -> clockHand == this -> numBufs - 1) {
		this -> clockHand = 0;
	}
	else {
		this -> clockHand++;
	}
}

/**
 * Allocates a free frame using the clock algorithm; if necessary, writing a dirty page back
	to disk. Throws BufferExceededException if all buffer frames are pinned. This private
	method will get called by the readPage() and allocPage() methods described below. Make
	sure that if the buffer frame allocated has a valid page in it, you remove the appropriate
	entry from the hash table.
 */
void BufMgr::allocBuf(FrameId & frame) 
{
	std::uint32_t pincount = 0;
	while(pincount <= numBufs - 1) {
		this -> advanceClock(); //Advance Clock Pointer
		frame = clockHand;
		if (bufDescTable[frame].valid == false) {
			return;
		}
		else {
			if (bufDescTable[frame].refbit == true) { //Checks if refbit has been set
				bufDescTable[frame].refbit = false;
				continue;
			}
			else {
				if(bufDescTable[frame].pinCnt > 0) { //Checks if Page has been pinned
					pincount++;
					continue;
				}
				else if(bufDescTable[frame].dirty == true) { //Checks Dirty Bit
					bufDescTable[frame].dirty = false;
					bufDescTable[frame].file -> writePage(bufPool[frame]);
				}
				//bufDescTable[clockHand].Set(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
				hashTable->remove(bufDescTable[frame].file, bufDescTable[frame].pageNo);
				return;
			}
		}
	}
	if(pincount > numBufs - 1) {
		throw BufferExceededException();
	}
	bufDescTable[frame].Clear();

	//frame = clockHand;
	//or 
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
 */
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo = 0;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];
	} catch (HashNotFoundException noHash) {
		try {
			allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo];
		} catch(BufferExceededException) {

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
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try{
		hashTable->lookup(file, pageNo, frameNo);

		if (bufDescTable[frameNo].pinCnt == 0) { // Pincount is already 0
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}

		if (dirty) { // If dirty == true, sets the dirty bit
			bufDescTable[frameNo].dirty = true;
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
	hashTable -> insert(file, newPage.page_number(), frameNo);
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
				throw PageNotPinnedException(bufDescTable[i].file->filename(), bufDescTable[i].pageNo, bufDescTable[i].frameNo);
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
