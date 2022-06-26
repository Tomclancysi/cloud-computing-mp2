/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

string serializeMemberList(const vector<MemberListEntry>& memberList);
vector<MemberListEntry> deserializeMemberList(char* p, size_t n);
vector<MemberListEntry> deserializeMemberList(const string& str);

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
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
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    waitingPingList = (notFullFilledPing*)malloc(sizeof(notFullFilledPing)*MAXQUEUELEN);
    waitingCount = 0;
    // this->pingPromise = new std::unordered_map<long, long>();
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        log->logNodeAdd(&this->memberNode->addr, &this->memberNode->addr);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
//    free(waitingPingList);
    printf("final wating list length %d\n", waitingCount);
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

MessageBody MP1Node::parseMessageBody(MsgTypes type, char *body){
    MessageBody msg;
    size_t n;
    switch(type){
        case MsgTypes::JOINREQ:
        msg.host = *(int*)body;
        body += sizeof(int);
        msg.port = *(short*)body;
        body += sizeof(short) + 1;
        msg.timeStamp = *(long*)body;
        break;
        
        case MsgTypes::JOINREP:
        msg.piggyBack = parsePiggyBackMsg(body);
        if(msg.piggyBack.size())
            this->updateMemberList(msg.piggyBack);
        // n = *(size_t*)body;
        // body += sizeof(size_t);
        // msg.memberList = deserializeMemberList(body, n);
        break;

        case MsgTypes::PING:
        case MsgTypes::PINGREQ:
        case MsgTypes::ACK:
        msg.host = *(int*)body;
        body += sizeof(int);
        msg.port = *(short*)body;
        body += sizeof(short);
        msg.discriptionID = *(long*)body;
        body += sizeof(long);
        if(type == MsgTypes::PINGREQ){
            msg.reqHost = *(int*)body;
            body += sizeof(int);
            msg.reqPort = *(short*)body;
            body += sizeof(short);
        }
        msg.piggyBack = parsePiggyBackMsg(body);
        
        if(msg.piggyBack.size()){
            // log->LOG(&this->memberNode->addr, to_string(msg.host).data());
            this->updateMemberList(msg.piggyBack);
            // printf("piggy back info %d, %d\n", msg.piggyBack[0].first, msg.piggyBack[0].second);
            // add the piggy back info to self memberlist
            // this part will be used in multiple region!!
            // int myHost = *(int*)(this->memberNode->addr.addr);
            // short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
            // auto &memList = this->memberNode->memberList;
            // for(auto mem : msg.piggyBack){
            //     if(myHost == mem.first && myPort == mem.second)
            //         continue;
            //     if(std::find_if(memList.begin(), memList.end(), [&](const MemberListEntry& ent){
            //         return ent.id == mem.first && ent.port == mem.second;
            //     }) == memList.end()){
            //         memList.emplace_back(mem.first, mem.second);
            //         Address dist = buildAddress(mem.first, mem.second);
            //         log->logNodeAdd(&this->memberNode->addr, &dist);
            //     }
            // }
        }
        break;
    }
    return msg;
}

string MP1Node::buildMessageBody(MessageHdr header, int host, short port, long discriptionID){
    // char *msg = (char*)malloc(sizeof(MessageHdr) + sizeof(int) + sizeof(short) + sizeof(long));
    size_t dataLength = sizeof(MessageHdr) + sizeof(int) + sizeof(short) + sizeof(long);
    string ret(dataLength, '0');
    char *msg = const_cast<char*>(ret.data());
    *((MessageHdr*)msg) = header;
    msg += sizeof(MessageHdr);
    *((int*)msg) = host;
    msg += sizeof(int);
    *((short*)msg) = port;
    msg += sizeof(short);
    *((long*)msg) = discriptionID;
    ret += buildPiggyBackMsg();
    return ret;
}

string MP1Node::buildMessageBody(MessageHdr header, size_t n, const vector<MemberListEntry>& memList){
    // string strMemList = serializeMemberList(memList);
    // size_t dataLength = sizeof(MessageHdr) + sizeof(size_t) + strMemList.size();
    size_t dataLength = sizeof(MessageHdr);
    string ret(dataLength, '0');
    // char *msg = (char*)malloc(sizeof(MessageHdr) + sizeof(size_t) + strMemList.size());
    char *msg = const_cast<char*>(ret.data());
    *((MessageHdr*)msg) = header;
    // msg += sizeof(header);
    // *((size_t*)msg) = n;
    // msg += sizeof(size_t);
    // memcpy(msg, strMemList.data(), strMemList.size());
    ret += buildPiggyBackMsg(10000);
    return ret;
}

string MP1Node::buildMessageBody(MessageHdr header, int fromHost, short fromPort, int reqHost, short reqPort, long discriptionID){
    size_t dataLength = sizeof(MessageHdr) + 2*sizeof(int) + 2*sizeof(short) + sizeof(long);
    string ret(dataLength, '0');
    char *msg = const_cast<char*>(ret.data());
    *((MessageHdr*)msg) = header;
    msg += sizeof(MessageHdr);
    *((int*)msg) = fromHost;
    msg += sizeof(int);
    *((short*)msg) = fromPort;
    msg += sizeof(short);
    *((long*)msg) = discriptionID;
    msg += sizeof(long);
    *((int*)msg) = reqHost;
    msg += sizeof(int);
    *((short*)msg) = reqPort;
    ret += buildPiggyBackMsg();
    return ret;
}

string MP1Node::buildPiggyBackMsg(int k/*=5*/){
    auto &memList = this->memberNode->memberList;
    vector<int> choosed;
    int t = std::min(memList.size(), (size_t)k);
    for(int i = 0; i < t; ++i)
        choosed.push_back(i);
    
    if(memList.size() > t){
        for(int i = t; i < memList.size(); ++i){
            int c = rand() % (1+i);
            if(c < t){
                choosed[c] = i;
            }
        }    
    }
    t += 1; // count myself
    size_t msgsize = (sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long)) * t + sizeof(int);
    string ret(msgsize, ' ');
    char *msg = const_cast<char*>(ret.data());
    *((int*)msg) = t;
    msg += sizeof(int);
    for(int c : choosed){
        *((int*)msg) = memList[c].getid();
        msg += sizeof(int);
        *((short*)msg) = memList[c].getport();
        msg += sizeof(short);
        *((long*)msg) = memList[c].gettimestamp();
        msg += sizeof(long);
        *((long*)msg) = memList[c].getheartbeat(); // 0 represent dead
        msg += sizeof(long);
    }
    int myHost = *(int*)(this->memberNode->addr.addr);
    short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
    long myTime = this->par->globaltime;
    long heart = 1l;
    *((int*)msg) = myHost;
    msg += sizeof(int);
    *((short*)msg) = myPort;
    msg += sizeof(short);
    *((long*)msg) = myTime;
    msg += sizeof(long);
    *((long*)msg) = heart; // 0 represent dead
    msg += sizeof(long);
    return ret;
}

vector<memberStatus> MP1Node::parsePiggyBackMsg(char* msg){
    int n = *((int*)msg);
    msg += sizeof(int);
    vector<memberStatus> ret;
    int host;
    short port;
    long timestamp, heart;
    for(int i = 0; i < n; i++){
        host = *((int*)msg); msg += sizeof(int);
        port = *((short*)msg); msg += sizeof(short);
        timestamp = *((long*)msg); msg += sizeof(long);
        heart = *((long*)msg); msg += sizeof(long);
        ret.push_back(make_tuple(host, port, timestamp, heart));
    }
    return ret;
}



Address MP1Node::buildAddress(int host, short port){
    Address dist;
    *((int*)dist.addr) = host;
    *(((short*)dist.addr) + 2) = port;
    return dist;
}

MemberListEntry* MP1Node::getAMember(){
    auto &memList = this->memberNode->memberList;
    if (memList.empty())
        return nullptr;
    // while(memList[this->curPingMemIndex].heartbeat == 0l)
    do{
        this->curPingMemIndex = (this->curPingMemIndex + 1) % memList.size();
    }while(memList[this->curPingMemIndex].heartbeat == 0l);
    
    // if (this->curPingMemIndex == 0){
    //     std::random_device rd;
    //     std::mt19937 g(rd());
    //     std::shuffle(memList.begin(), memList.end(), g);
    // }
    return &memList[this->curPingMemIndex];
}

void MP1Node::encountJoinReq(MessageBody body){
    // this should be the leader.
    auto & memList = this->memberNode->memberList;
    
    // send join rep, tell it all member in this group
    MessageHdr header;
    header.msgType = MsgTypes::JOINREP;
    Address dist = buildAddress(body.host, body.port);
    
    string msg = buildMessageBody(header, memList.size(), memList);
    char* ip = (char*)(&body.host);
    printf("accepted join req from IP %d.%d.%d.%d, reply it.\n", *ip, *(ip+1), *(ip+2), *(ip+3));
    // memList.emplace_back(body.host, body.port, body.heartBeat, body.timeStamp);
    this->updateMemberList({make_tuple(body.host, body.port, body.timeStamp, 1l)}); // use update to update the memberlist not just add to list!
#ifdef DEBUGLOG
    char buf[1024];
    sprintf(buf, "accepted join req from IP %d.%d.%d.%d, reply it.\n", *ip, *(ip+1), *(ip+2), *(ip+3));
    log->LOG(&memberNode->addr, buf);
#endif
    emulNet->ENsend(&memberNode->addr, &dist, const_cast<char*>(msg.data()), msg.size());
}

void MP1Node::encountJoinRep(MessageBody body){
    // in the group, update the memberlist
    if(this->memberNode->inGroup){
        // already in group, dumplicate response
        return ;
    }
    // this->memberNode->memberList = body.memberList;
    // auto address = this->getJoinAddress();
    // this->memberNode->memberList.emplace_back(*(int*)address.addr, *((short*)address.addr + 2), 0l, 0l);
    this->memberNode->inGroup = true;
    // this->updateMemberList(body.piggyBack);
    // for(auto m : this->memberNode->memberList){// initalize the member list , log all member
    //      Address memberAddress = buildAddress(m.getid(), m.getport());
    //      log->logNodeAdd(&this->memberNode->addr, &memberAddress);
    // }
    
#ifdef DEBUGLOG
    // char *buf = (char*)malloc(1024);
    // for(auto m : this->memberNode->memberList){
    //     char* ip = (char*)(&(m.id));
    //     sprintf(buf, "MemberList %d.%d.%d.%d, reply it.", *ip, *(ip+1), *(ip+2), *(ip+3));
    //     log->LOG(&memberNode->addr, buf);
    // }
    // free(buf);
#endif
}

void MP1Node::encountPing(MessageBody body){
    // return ACK
    char* ip = (char*)(&(body.host));
    MessageHdr header;
    header.msgType = MsgTypes::ACK;
    int myHost = *(int*)(this->memberNode->addr.addr);
    short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
    Address distination = buildAddress(body.host, body.port);
    string msg = buildMessageBody(header, myHost, myPort, body.discriptionID);
    emulNet->ENsend(&this->memberNode->addr, &distination, const_cast<char*>(msg.data()), msg.size());
    // printf("Ping from %d.%d.%d.%d, send ack to it.\n", *ip, *(ip+1), *(ip+2), *(ip+3));
}

void MP1Node::encountAck(MessageBody body){
    // printf("receive one ack\n");
    char* ip = (char*)(&(body.host));
    long ID = body.discriptionID;
    size_t idx = 0;
    while (idx < waitingCount && waitingPingList[idx].discriptionID != ID) ++idx;
    if (idx == waitingCount)return; // already late
    if(waitingPingList[idx].type == MsgTypes::PINGREQ){
        // reply ack to the origin
        MessageHdr header;
        header.msgType = MsgTypes::ACK;
        int myHost = *(int*)(this->memberNode->addr.addr);
        short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
        Address distination = buildAddress(waitingPingList[idx].host, waitingPingList[idx].port);
        string msg = buildMessageBody(header, myHost, myPort, waitingPingList[idx].discriptionID);
        emulNet->ENsend(&this->memberNode->addr, &distination, const_cast<char*>(msg.data()), msg.size());
    }
    
    // remove the record
    while(idx + 1 < waitingCount){
        waitingPingList[idx] = waitingPingList[idx+1];
        ++idx;
    }
    waitingCount--;
}

void MP1Node::encountPingReq(MessageBody body){
    // printf("receive a ping req\n");
    // this node should record the address and when receive a ack, check if it is needed to reply the ack
    // CAUTION use same discription id
    // send a simple ping but record ping req
    Address distination = buildAddress(body.reqHost, body.reqPort);
    MessageHdr header;
    header.msgType = MsgTypes::PING;
    int myHost = *(int*)(this->memberNode->addr.addr);
    short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
    string msg = buildMessageBody(header, myHost, myPort, body.discriptionID);
    emulNet->ENsend(&this->memberNode->addr, &distination, const_cast<char*>(msg.data()), msg.size());
    // record as ping req
    waitingPingList[waitingCount].discriptionID = body.discriptionID;
    waitingPingList[waitingCount].timestamp = this->par->globaltime;
    waitingPingList[waitingCount].host = body.host;
    waitingPingList[waitingCount].port = body.port;
    waitingPingList[waitingCount].type = MsgTypes::PINGREQ;
    waitingCount++;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MsgTypes type = (*(MessageHdr*)data).msgType;
    // printf("accept message, type=%d\n", type);
    data += sizeof(MessageHdr);
    MessageBody body = parseMessageBody(type, data);
    switch (type)
    {
    case MsgTypes::JOINREP: // yeah!成功加入组，看看组里还有谁？
        this->encountJoinRep(body);
        break;
    case MsgTypes::JOINREQ: // 请求加入组
        this->encountJoinReq(body);
        break;
    case MsgTypes::PING:
        this->encountPing(body);    
        break;
    case MsgTypes::PINGREQ:
        // 直接回复ping 相应地址
        this->encountPingReq(body);
        break;
    case MsgTypes::ACK:
        // 两种情况，如果是PingREQ则转发ack
        this->encountAck(body);
        break;
    case MsgTypes::DUMMYLASTMSGTYPE:
        break;
    default:
        break;
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // return;
	/*
	 * Your code goes here
	 */
    auto curTime = this->par->globaltime;
    int myHost = *(int*)(this->memberNode->addr.addr);
    short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
    // cheak and remove the expired ping
    size_t j = 0;
    auto &memList = this->memberNode->memberList;
    for(size_t i = 0; i < waitingCount; ++i){
        // if is indirect ping, we need to do nothing
        if(waitingPingList[i].type == MsgTypes::PINGREQ){
            if(curTime - waitingPingList[i].timestamp <= 2)
                waitingPingList[j++] = waitingPingList[i];
            continue;
        }
        if (curTime - waitingPingList[i].timestamp >= TREMOVE){
            // remove, spread remove msg?
            auto iter = find_if(memList.begin(), memList.end(), [&](const MemberListEntry ent){
                return ent.id == waitingPingList[i].host && ent.port == waitingPingList[i].port;
            });
            if (iter != memList.end()){
                if(iter->heartbeat == 1l){
                    Address dist = buildAddress(waitingPingList[i].host, waitingPingList[i].port);
                    // log->LOG(&this->memberNode->addr, string("remove for time out").data());
                    log->logNodeRemove(&this->memberNode->addr, &dist);
                }
                iter->heartbeat = 0l;
                iter->timestamp = curTime;
            }
            
        }
        else if(curTime - waitingPingList[i].timestamp >= TFAIL){
            // fail and spread 
            
            waitingPingList[j++] = waitingPingList[i];
        }
        else if(curTime - waitingPingList[i].timestamp >= 2){
            // send indirect ping-req
            int k = std::min((size_t)5, memList.size());
            while (k--){
                auto pm = getAMember();
                // 这一坨shit就是因为api没设计好， 要不然就是一行，但是不想改了 
                // when sending indirect request, we should use the discription id of the old, so it can be fullfilled in the future.
                // when one node receive a ping req it need to reply so need to remember the source
                Address distination;
                *((int*)distination.addr) = pm->getid();
                *(((short*)distination.addr) + 2) = pm->getport();
                MessageHdr header;
                header.msgType = MsgTypes::PINGREQ;
                // string msg = buildMessageBody(header, myHost, myPort, waitingPingList[i].discriptionID);
                string msg = buildMessageBody(header, myHost, myPort, waitingPingList[i].host, waitingPingList[i].port, waitingPingList[i].discriptionID);
                emulNet->ENsend(&this->memberNode->addr, &distination, const_cast<char*>(msg.data()), msg.size());
                
            }
            waitingPingList[j++] = waitingPingList[i];
        }
        else
            waitingPingList[j++] = waitingPingList[i];
    }
    waitingCount = j;
    // SWIM style
    // Ping another Member at a random choice
    if (memList.size() >= 1){
        // ping this member, msg formatter: header,address,
        // dose swim gossip its fully memberlist with others
        auto p = getAMember();
        if(p == nullptr)
            return;
        
        int distHost = p->getid();
        short distPort = p->getport();
        Address distination;
        *((int*)distination.addr) = distHost;
        *(((short*)distination.addr) + 2) = distPort;

        // this is gossip style, we will update memberlist in else where
        /*
        string strMemList = serializeMemberList(memList);
        size_t msgsize = sizeof(MessageHdr) + sizeof(distination.addr) + strMemList.size() + 1;
        MessageHdr* msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create DIRECTPING message: format of data is {Header MyAddress MemberList}
        msg->msgType = MsgTypes::DIRECTPING;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

        // send PING message to introducer member
        emulNet->ENsend(&memberNode->addr, &distination, (char *)msg, msgsize);
        // 怎么判断ping超时呢？
        free(msg);
        */
        MessageHdr header;
        header.msgType = MsgTypes::PING;
        
        long discriptionID = (myHost * 1234567l) ^ (myPort * 4567890) ^ curTime ^ rand(); // ip host + time 的随机数
        string msg = buildMessageBody(header, myHost, myPort, discriptionID);
        emulNet->ENsend(&this->memberNode->addr, &distination, const_cast<char*>(msg.data()), msg.size());
        // record current ping result

        waitingPingList[waitingCount].discriptionID = discriptionID;
        waitingPingList[waitingCount].timestamp = curTime;
        waitingPingList[waitingCount].host = distHost;
        waitingPingList[waitingCount].port = distPort;
        waitingPingList[waitingCount].type = MsgTypes::PING;
        waitingCount++;
        
    }


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


string serializeMemberList(const vector<MemberListEntry>& memberList){
    size_t n = memberList.size();
    size_t msgsize = n * (6 + sizeof(long)*2);
    string res(msgsize, ' ');
    auto p = res.data();
    for(size_t i = 0; i < n; i ++){
        *((int*)p) = memberList[i].id;
        p += sizeof(int);
        *((short*)p) = memberList[i].port;
        p += sizeof(short);
        *((long*)p) = memberList[i].heartbeat;
        p += sizeof(long);
        *((long*)p) = memberList[i].timestamp;
        p += sizeof(long);
    }
    return res;
}

vector<MemberListEntry> deserializeMemberList(const string& str){
    assert(str.size() % (sizeof(int) + sizeof(short) + 2 * sizeof(long)) == 0);
    vector<MemberListEntry> memberList;
    auto p = str.data();
    size_t n = str.size() / (sizeof(int) + sizeof(short) + 2 * sizeof(long));
    for(size_t i = 0; i < n; i ++){
        memberList[i].id = *((int*)p);
        p += sizeof(int);
        memberList[i].port = *((short*)p);
        p += sizeof(short);
        memberList[i].heartbeat = *((long*)p);
        p += sizeof(long);
        memberList[i].timestamp = *((long*)p);
        p += sizeof(long);
    }
    return memberList;
}

vector<MemberListEntry> deserializeMemberList(char* p, size_t n){
    vector<MemberListEntry> memberList(n);
    for(size_t i = 0; i < n; i ++){
        memberList[i].id = *((int*)p);
        p += sizeof(int);
        memberList[i].port = *((short*)p);
        p += sizeof(short);
        memberList[i].heartbeat = *((long*)p);
        p += sizeof(long);
        memberList[i].timestamp = *((long*)p);
        p += sizeof(long);
    }
    return memberList;
}

void MP1Node::updateMemberList(const vector<memberStatus>& status){
    int myHost = *(int*)(this->memberNode->addr.addr);
    short myPort = *(((short*)(this->memberNode->addr.addr)) + 2);
    auto &memList = this->memberNode->memberList;
    for(auto mem : status){
        auto a = std::get<0>(mem);
        auto b = std::get<1>(mem);
        auto c = std::get<2>(mem);
        auto d = std::get<3>(mem);
        if(myHost == a && myPort == b)
            continue;
        auto iter = std::find_if(memList.begin(), memList.end(), [&](const MemberListEntry& ent){
            return ent.id == a && ent.port == b;
        });
        Address dist = buildAddress(a, b);
        if(iter == memList.end()){
            memList.emplace_back(a, b, d, c);
            if(d == 1l)
                log->logNodeAdd(&this->memberNode->addr, &dist);
        }
        else{
            // if exist check the time
            if (iter->timestamp < c){
                // this message is newer than me
                iter->timestamp = c;
                if(d == 1l && iter->heartbeat == 0l){
                    log->logNodeAdd(&this->memberNode->addr, &dist);
                }
                else if(d == 0l && iter->heartbeat == 1l){
                    // log->LOG(&this->memberNode->addr, string("remove form new message").data());
                    log->logNodeRemove(&this->memberNode->addr, &dist);
                }
                iter->heartbeat = d;
            }
            else if(iter->timestamp == c){
                if(d == 0l && iter->heartbeat == 1l){
                    // log->LOG(&this->memberNode->addr, string("remove form equal message").data());
                    log->logNodeRemove(&this->memberNode->addr, &dist);
                    iter->heartbeat = d;
                }
            }
        }
    }
}