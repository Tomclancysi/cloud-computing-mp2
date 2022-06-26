/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#include<unordered_map>
#include<utility>

#define QUORUM 2
// class CallBack{
// public:
// 	int count;
// 	Log *log;
// 	virtual void operator()() = 0;
// };

// class ClientCreateCallBack : public CallBack{
// public:
// 	Address addr;
// 	bool isCordinatior;
// 	int transID;
// 	string key;
// 	string value;
// 	ClientCreateCallBack(Address addr,
// 		bool isCordinatior,
// 		int transID,
// 		string key,
// 		string value){
// 	}
// 	void operator()(){
// 		if(this->count == 0){
// 			this->log->logCreateSuccess();
// 		}
// 		else{

// 		}
// 	}
// };

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
	int myHost;
	int myPort;
	Node self;
	// vector<Message> msgQueue;
	unordered_map<int, pair<int, Message>> replyCount;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message, Address dist);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	size_t findPredecessor(size_t hashcode);
	void replyMessage(int transID, Address fromAddress, Address toAddress, MessageType type, bool success);

	~MP2Node();
};

#endif /* MP2NODE_H_ */
