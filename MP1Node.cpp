/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <string.h>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
	this->curtime = 0;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }
    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	short port = *(short*)(&memberNode->addr.addr[4]);

	this->selfid = id;
	this->selfport = port;
	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	Msg_t *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;	
    }
    else {
        size_t msgsize = sizeof (Msg_t);
        msg = (Msg_t *) malloc(msgsize);

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->hdr.msgType = JOINREQ;
	memcpy(&msg->to_addr, &joinaddr->addr, sizeof(joinaddr->addr));
	memcpy(&msg->my_addr, &memberNode->addr.addr, sizeof(memberNode->addr.addr));
	msg->heartbeat = memberNode->heartbeat;
	msg->mem_table_num_entries = 0;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }
	insertMembership(&memberNode->addr, memberNode->heartbeat);
    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
	memberNode->inited = false;
	memberNode->addr.init();
	memberNode->inGroup = false;
	memberNode->memberList.clear();	
	this->timeoutnodes.clear();
	delete(memberNode);
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }
    setcurtime(getcurtime()+1);
    checkValidation();
    deleteFailNodeMem();
    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	Msg_t		*recvmsg = (Msg_t *)data;
	Msg_t		*respmsg;
	long 		nodeHB = recvmsg->heartbeat;
	Address 	*src_addr = new Address();
	size_t		msg_size;
	MemberListEntry *addr;
	int		num;

	memcpy(&src_addr->addr, &recvmsg->my_addr, sizeof(recvmsg->my_addr));

	switch (recvmsg->hdr.msgType) {
		case JOINREQ:
			/* formulate resp msg */
			msg_size = sizeof(Msg_t) + sizeof(MemberListEntry)*memberNode->memberList.size();
			respmsg =(Msg_t *) malloc(msg_size);
			respmsg->hdr.msgType = JOINREP;
			memcpy(&respmsg->to_addr, &recvmsg->my_addr, sizeof(recvmsg->my_addr));
			memcpy(&respmsg->my_addr, &memberNode->addr.addr, sizeof(respmsg->my_addr));
			respmsg->heartbeat = memberNode->heartbeat;	
			addr = &respmsg->mem_table;
			num = 0;
			for (int i=0; i<memberNode->memberList.size(); i++) {
				memcpy(addr, &memberNode->memberList[i], sizeof(MemberListEntry));
				addr++;
				num++;
			}
			msg_size = sizeof(Msg_t) + sizeof(MemberListEntry)*num;
			respmsg->mem_table_num_entries = num;
			/* update membership list */	
			insertMembership(src_addr, nodeHB);
			/* send out resp message */
			emulNet->ENsend(&memberNode->addr, src_addr, (char *)respmsg, msg_size);	
			free(respmsg);
			break;
		case JOINREP:
			memberNode->inGroup = true;	
			/* TO-DO: do we really need this insert? */
			//insertMembership(src_addr, nodeHB);	
			mergeMembership(&recvmsg->mem_table, recvmsg->mem_table_num_entries);
			break;
		case MEMBERSHIP:
			//updateSingleEntry(src_addr, nodeHB);
			/* invalidate membership entries */	
			mergeMembership(&recvmsg->mem_table, recvmsg->mem_table_num_entries);
			break;
	}
	delete(src_addr);
}

long MP1Node::constructIDPORT(MemberListEntry *en) {
	int id = en->getid();
	short port = en->getport();
	return (long)(id<<8+port);
}

void MP1Node::checkValidation(void) {
	for (int i=0; i<memberNode->memberList.size(); i++) {
		MemberListEntry *en = &memberNode->memberList[i];
		long idport = constructIDPORT(en);
		if (getcurtime() - en->gettimestamp() > TFAIL && timeoutnodes.find(idport)==timeoutnodes.end()) {
			memberNode->timeOutCounter++;
			timeoutnodes.insert(idport);
		}
	}
}

void MP1Node::deleteFailNodeMem(void) {
	vector<MemberListEntry>::iterator itr;
	for (itr=memberNode->memberList.begin(); itr!=memberNode->memberList.end();) {
		if (getcurtime() - itr->gettimestamp() > TREMOVE) {		
			string straddr = to_string(itr->getid()) + ":" + to_string(itr->getport());
			int pos = itr - memberNode->memberList.begin();
			MemberListEntry *en = &(memberNode->memberList[pos]);
			long idport = constructIDPORT(en);
			timeoutnodes.erase(idport);
			Address *remove_addr = new Address(straddr);
			log->logNodeRemove(&memberNode->addr, remove_addr);
			delete(remove_addr);
			itr = memberNode->memberList.erase(itr);
		} else {
			++itr;
		}
	}
}

void MP1Node::insertMembership(Address *newnode_addr, long nodeHB) {
	int id;
	short port;
	memcpy(&id, &newnode_addr->addr[0], sizeof(int));
	memcpy(&port, &newnode_addr->addr[4], sizeof(short));
	MemberListEntry en = MemberListEntry(id, port, nodeHB, getcurtime());
	memberNode->memberList.push_back(en);
	log->logNodeAdd(&memberNode->addr, newnode_addr);
}

void MP1Node::mergeMembership(MemberListEntry *entry_ptr, int size) {
	for (int i=0; i<size; i++) {
		int j=0;
		int id=entry_ptr->getid();
		long nodeHB=entry_ptr->heartbeat;
		short port=entry_ptr->getport();
		bool found = false;
		for (j=0; j<memberNode->memberList.size(); j++) {	
			MemberListEntry *en = &memberNode->memberList[j];	
			long idport = constructIDPORT(en);
			if (en->getid()==id && en->getport()==port) {
				found = true;
				if (en->heartbeat < nodeHB &&
				   timeoutnodes.find(idport)==timeoutnodes.end()) {	
					en->settimestamp(getcurtime());
					en->heartbeat = nodeHB;
				}
				break;
			}
		}
		if (!found) {
			MemberListEntry en = MemberListEntry(id, port, nodeHB, getcurtime());
			memberNode->memberList.push_back(en);
			string addrstr = to_string(id)+":"+to_string(port);
			Address *newnode_addr = new Address(addrstr);
			log->logNodeAdd(&memberNode->addr, newnode_addr);
			delete(newnode_addr);
		}
		entry_ptr++;
	}
}

long MP1Node::getcurtime() {
	return curtime;
}

void MP1Node::setcurtime(long time) {
	curtime = time;
}

void MP1Node::generateRandNeighbors(vector<int> &neighbors) {
	
	int len = memberNode->memberList.size();
	int rn = rand()%len;
	int step=0;
	for (int i=rn; step<(len+1)/2; step++, i++) {
		neighbors.push_back(i%len);
	}

	//for (int i=0; i<memberNode->memberList.size(); i++) neighbors.push_back(i);
}
/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
	Msg_t		*to_send;
	size_t		msg_size;
	vector<int>	neighbors;
	int		num=0;
	/* update membership entry of itself */
	memberNode->heartbeat++;
	for (int i=0; i<memberNode->memberList.size(); i++) {
		if (memberNode->memberList[i].getid()==selfid &&
		    memberNode->memberList[i].getport()==selfport) {
			memberNode->memberList[i].setheartbeat(memberNode->heartbeat);
			memberNode->memberList[i].settimestamp(getcurtime());
			break;
		}
	}
	/* send out membership list */
	generateRandNeighbors(neighbors);
	msg_size = sizeof(Msg_t) + sizeof(MemberListEntry)*memberNode->memberList.size();
	to_send = (Msg_t *)malloc(msg_size);
	to_send->hdr.msgType = MEMBERSHIP;
	to_send->heartbeat = memberNode->heartbeat;	
	MemberListEntry *addr = &to_send->mem_table;

	for (int i=0; i<memberNode->memberList.size(); i++) {
		MemberListEntry *en = &memberNode->memberList[i];
		long idport = constructIDPORT(en);
		if (timeoutnodes.find(idport)!=timeoutnodes.end()) continue;
		memcpy(addr, &memberNode->memberList[i], sizeof(MemberListEntry));
		addr++;
		num++;
	}
	to_send->mem_table_num_entries = num;
	msg_size = sizeof(Msg_t) + num * sizeof(MemberListEntry);
   	for (int i=0; i<neighbors.size(); i++) {
		string straddr;
		MemberListEntry en = memberNode->memberList[neighbors[i]];
		straddr = std::to_string(en.getid()) + ":" + std::to_string(en.getport());
		Address *dst_addr = new Address(straddr);
		emulNet->ENsend(&memberNode->addr, dst_addr, (char *)to_send, msg_size);
		delete(dst_addr);
	}
	free(to_send);

	return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();	
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
