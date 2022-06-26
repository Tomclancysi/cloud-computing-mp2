/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"


/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	this->self = Node(this->memberNode->addr);
	this->myHost = *(int*)address->addr;
	this->myPort = *(((short*)address->addr)+2);
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	// printf("delete");
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	// auto self = Node(this->memberNode->addr);
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());
	if(curMemList.size() != this->ring.size())
		change = true;
	else{
		for(size_t i = 0; i < curMemList.size(); ++i)
			if(curMemList[i].getHashCode() != this->ring[i].getHashCode()){
				change = true;
				break;
			}
	}

	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED, UPDATE THE FINGER TABLE AND SUCCESSOR PREDESSOR
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	if(change){
		this->ring = curMemList;
		auto ringSize = this->ring.size();
		for(size_t i = 0; i < ringSize; ++i){
			if (this->ring[i].getHashCode() == self.getHashCode()){
				// need to replica the key
				vector<Node> newHasMyReplicas = {this->ring[(i+1) % ringSize], this->ring[(i+2) % ringSize]}; // the successor
				vector<Node> newHaveReplicasOf = {this->ring[(i-1+ringSize) % ringSize], this->ring[(i-2+ringSize) % ringSize]}; // the predcessor
				// if needed to update the key, then update
				for(auto &rep : newHasMyReplicas){
					bool flag = false;
					for(auto& req: hasMyReplicas)
						if(req.getHashCode() == rep.getHashCode())
							flag = true;
					if(!flag){
						// send message update my hashtable
						for(auto& kv : this->ht->hashTable){
							log->LOG(&this->memberNode->addr, "send the querydfsdfsdf");
							Message msg(rand(), this->memberNode->addr, CREATE, kv.first, kv.second, ReplicaType(i));
							Address* dist = rep.getAddress();
							dispatchMessages(msg, *dist);
						}
					}
				}
				this->hasMyReplicas = newHasMyReplicas;
				this->haveReplicasOf = newHaveReplicasOf;
				break;
			}
		}
	}
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	auto curTime = this->par->getcurrtime();
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		if(this->memberNode->memberList[i].heartbeat == 0l /*&& curTime - this->memberNode->memberList[i].timestamp >= 20*/) // judge if it is out of group
			continue;
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	// need it self maybe ?
	Address addressOfMySelf = this->memberNode->addr;
	curMemList.emplace_back(Node(addressOfMySelf));
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

void MP2Node::dispatchMessages(Message msg, Address dist){
	string str = msg.toString();
	emulNet->ENsend(&this->memberNode->addr, &dist, str);
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	printf("client create\n");
	auto nodes = findNodes(key);
	
	auto transID = (myHost * 1234567) ^ (myPort * 4567890) ^ this->par->getcurrtime() ^ rand(); // ip host + time 的随机数
	for(int i = 0; i < nodes.size(); ++i){
		Message msg(transID, this->memberNode->addr, CREATE, key, value, ReplicaType(i));
		if(i == 0){
			// this->replyCount[transID] = make_pair(3, msg);
			// this->replyCount.insert(make_pair(transID, make_pair(3, msg)));
			this->replyCount.emplace(transID, make_pair(QUORUM, msg)); // emplace 直接调用构造函数，而不是像push_back先默认构造然后赋值
			timestamp[transID] = this->par->getcurrtime();
		}
		Address* dist = nodes[i].getAddress();
		dispatchMessages(msg, *dist);
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	printf("client read\n");
	auto nodes = findNodes(key);
	
	auto transID = (myHost * 1234567) ^ (myPort * 4567890) ^ this->par->getcurrtime() ^ rand(); // ip host + time 的随机数
	for(int i = 0; i < nodes.size(); ++i){
		Message msg(transID, this->memberNode->addr, READ, key);
		if(i == 0){
			this->replyCount.emplace(transID, make_pair(QUORUM, msg)); // emplace 直接调用构造函数，而不是像push_back先默认构造然后赋值
			timestamp[transID] = this->par->getcurrtime();
		}
		Address* dist = nodes[i].getAddress();
		dispatchMessages(msg, *dist);
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	auto nodes = findNodes(key);
	
	auto transID = (myHost * 1234567) ^ (myPort * 4567890) ^ this->par->getcurrtime() ^ rand(); // ip host + time 的随机数
	for(int i = 0; i < nodes.size(); ++i){
		Message msg(transID, this->memberNode->addr, UPDATE, key, value, ReplicaType(i));
		if(i == 0){
			this->replyCount.emplace(transID, make_pair(QUORUM, msg)); // emplace 直接调用构造函数，而不是像push_back先默认构造然后赋值
			timestamp[transID] = this->par->getcurrtime();
		}
		Address* dist = nodes[i].getAddress();
		dispatchMessages(msg, *dist);
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	auto nodes = findNodes(key);
	
	auto transID = (myHost * 1234567) ^ (myPort * 4567890) ^ this->par->getcurrtime() ^ rand(); // ip host + time 的随机数
	for(int i = 0; i < nodes.size(); ++i){
		Message msg(transID, this->memberNode->addr, DELETE, key);
		if(i == 0){
			this->replyCount.emplace(transID, make_pair(QUORUM, msg)); // emplace 直接调用构造函数，而不是像push_back先默认构造然后赋值
			timestamp[transID] = this->par->getcurrtime();
		}
		Address* dist = nodes[i].getAddress();
		dispatchMessages(msg, *dist);
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	printf("server create\n");
	return this->ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return this->ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return this->ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return this->ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::replyMessage(int transID, Address fromAddress, Address toAddress, MessageType type, bool success){
	Message msg(transID, fromAddress, type, success);
	emulNet->ENsend(&this->memberNode->addr, &toAddress, msg.toString());
}
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented. MAIN LOOP
	 */
	char * data;
	int size;
	if(this->par->getcurrtime() % 8 == 0){
		int curTime = this->par->getcurrtime();
		auto iter = this->replyCount.begin();
		while(iter != this->replyCount.end()){
			auto& msg = iter->second.second;
			if(curTime - timestamp[msg.transID] >= 18){
				if(msg.type == CREATE)
					log->logCreateFail(&this->memberNode->addr, true, msg.transID, msg.key, msg.value);
				else if(msg.type == READ)
					log->logReadFail(&this->memberNode->addr, true, msg.transID, msg.key);
				else if(msg.type == UPDATE)
					log->logUpdateFail(&this->memberNode->addr, true, msg.transID, msg.key, msg.value);
				else if(msg.type == DELETE)
					log->logDeleteFail(&this->memberNode->addr, true, msg.transID, msg.key);
				iter = this->replyCount.erase(iter);
			}
			else			
				iter++;
		}
	}
	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		Message msg(message);
		if(msg.type == REPLY || msg.type == READREPLY){
			auto iter = this->replyCount.find(msg.transID);
			if(iter == this->replyCount.end())
				return;
			auto& value = iter->second;
			
			// 这里需要根据返回msg进行一点更新
			if(msg.type == READREPLY && (msg.value != "" || value.first == QUORUM)){
				value.second.value = msg.value;
			}
			
			if((msg.type == REPLY && msg.success) || (msg.type == READREPLY && msg.value != ""))
				value.first -= 1;
			if(value.first <= 0){
				switch (value.second.type)
				{
				case MessageType::CREATE:
					log->logCreateSuccess(&this->memberNode->addr, true, value.second.transID, value.second.key, value.second.value);
					break;
				case MessageType::READ:
					log->logReadSuccess(&this->memberNode->addr, true, msg.transID, value.second.key, value.second.value);
					break;
				case MessageType::UPDATE:
					log->logUpdateSuccess(&this->memberNode->addr, true, msg.transID, value.second.key, value.second.value);
					break;
				case MessageType::DELETE:
					log->logDeleteSuccess(&this->memberNode->addr, true, msg.transID, value.second.key);
					break;
				default:
					break;
				}
				this->replyCount.erase(iter);
			}
		}
		else if(msg.type == CREATE){
			bool suc = createKeyValue(msg.key, msg.value, msg.replica);
			if(suc)
				log->logCreateSuccess(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
			else
				log->logCreateFail(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
			replyMessage(msg.transID, this->memberNode->addr, msg.fromAddr, REPLY, suc);
		}
		else if(msg.type == READ){
			auto result = readKey(msg.key);
			if(result == "")
				log->logReadFail(&this->memberNode->addr, false, msg.transID, msg.key);
			else
				log->logReadSuccess(&this->memberNode->addr, false, msg.transID, msg.key, result);
			Message retmsg(msg.transID, this->memberNode->addr, READREPLY, msg.key);
			retmsg.value = result;
			emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, retmsg.toString());
		}
		else if(msg.type == UPDATE){
			auto result = updateKeyValue(msg.key, msg.value, msg.replica);
			if(result)
				log->logUpdateSuccess(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
			else
				log->logUpdateFail(&this->memberNode->addr, false, msg.transID, msg.key, msg.value);
			Message retmsg(msg.transID, this->memberNode->addr, REPLY, result);
			emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, retmsg.toString());
		}
		else if(msg.type == DELETE){
			auto result = deletekey(msg.key);
			if(result)
				log->logDeleteSuccess(&this->memberNode->addr, false, msg.transID, msg.key);
			else
				log->logDeleteFail(&this->memberNode->addr, false, msg.transID, msg.key);
			Message retmsg(msg.transID, this->memberNode->addr, REPLY, result);
			emulNet->ENsend(&this->memberNode->addr, &msg.fromAddr, retmsg.toString());
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
}


size_t MP2Node::findPredecessor(size_t hashcode){
	for(size_t i = 0; i < this->ring.size(); ++i){
		if(this->ring[i].getHashCode() >= hashcode){
			return i;
		}
	}
	return 0;
}