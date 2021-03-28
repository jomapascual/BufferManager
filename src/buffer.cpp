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


BufMgr::~BufMgr() {
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

void BufMgr::allocBuf(FrameId & frame) 
{
	std::uint32_t pincount = 0;
	while(pincount <= numBufs - 1) {
		this -> advanceClock(); //Advance Clock Pointer
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

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	// alloc empty page in the specified file 
	Page* newPage = &file->allocatePage();
	FrameId frameNo = 0;

	// call allocBuf() to obtain buffer pool frame
	allocBuf(frameNo);
	// get frame from buffer pool
	// bufPool[frameNo] = newPage;
	// insert into hashtable
	hashTable -> insert(file, newPage->page_number(), frameNo);
	// call Set() to set frame properly
	bufDescTable[clockHand].Set(file, newPage->page_number());
	pageNo = newPage->page_number();
	page = &bufPool[frameNo];

}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNum;
	try {
		// make sure page to be deleted is allocated in buffer pool
		hashtable->lookup(file, PageNo, frameNum);
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
