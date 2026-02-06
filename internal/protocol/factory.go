package protocol

import (
	"time"
)

func newHeader(msgType MessageType, senderID string, senderType NodeType, seqNum int64) *MessageHeader {
	return &MessageHeader{
		Type:        msgType,
		Timestamp:   time.Now().UnixNano(),
		SenderId:    senderID,
		SequenceNum: seqNum,
		SenderType:  senderType,
	}
}

// TCP unicast from Producer to Leader
func NewProduceMsg(senderID string, topic string, producerAddr string, seqNum int64) *ProduceMsg {
	return &ProduceMsg{
		ProduceMessage: &ProduceMessage{
			Header:          newHeader(MessageType_PRODUCE, senderID, NodeType_PRODUCER, seqNum),
			Topic:           topic,
			ProducerAddress: producerAddr,
		},
	}
}

// UDP unicast from Producer to Broker
func NewDataMsg(senderID string, topic string, data []byte, seqNum int64) *DataMsg {
	return &DataMsg{
		DataMessage: &DataMessage{
			Header: newHeader(MessageType_DATA, senderID, NodeType_PRODUCER, seqNum),
			Topic:  topic,
			Data:   data,
		},
	}
}

// TCP unicast from Consumer to Leader
func NewConsumeMsg(senderID string, topic string, consumerAddr string, seqNum int64) *ConsumeMsg {
	return &ConsumeMsg{
		ConsumeMessage: &ConsumeMessage{
			Header:          newHeader(MessageType_CONSUME, senderID, NodeType_CONSUMER, seqNum),
			Topic:           topic,
			ConsumerAddress: consumerAddr,
		},
	}
}

// TCP unicast from Consumer to assigned Broker (for stream subscription after registration)
func NewSubscribeMsg(senderID, topic, consumerID, consumerAddr string, enableProcessing bool, analyticsWindowSeconds, analyticsIntervalMs int32) *SubscribeMsg {
	return &SubscribeMsg{
		SubscribeMessage: &SubscribeMessage{
			Header:                 newHeader(MessageType_SUBSCRIBE, senderID, NodeType_CONSUMER, 0),
			Topic:                  topic,
			ConsumerId:             consumerID,
			ConsumerAddress:        consumerAddr,
			EnableProcessing:       enableProcessing,
			AnalyticsWindowSeconds: analyticsWindowSeconds,
			AnalyticsIntervalMs:    analyticsIntervalMs,
		},
	}
}

// TCP unicast from Broker to Consumer
func NewResultMsg(senderID string, topic string, data []byte, offset, seqNum int64) *ResultMsg {
	return &ResultMsg{
		ResultMessage: &ResultMessage{
			Header: newHeader(MessageType_RESULT, senderID, NodeType_BROKER, seqNum),
			Topic:  topic,
			Data:   data,
			Offset: offset,
		},
	}
}

// TCP unicast for Leader to Producer/Consumer
// TCP unicast for Broker to Leader
// UDP multicast for Leader to Brokers
func NewHeartbeatMsg(senderID string, senderType NodeType, seqNum int64, viewNumber int64) *HeartbeatMsg {
	return &HeartbeatMsg{
		HeartbeatMessage: &HeartbeatMessage{
			Header:     newHeader(MessageType_HEARTBEAT, senderID, senderType, seqNum),
			ViewNumber: viewNumber,
		},
	}
}

// TCP unicast from Broker to specific sender
// UDP multicast from Broker to all (if sender unknown)
func NewNackMsg(senderID, targetSenderID string, senderType NodeType, fromSeq, toSeq int64) *NackMsg {
	return &NackMsg{
		NackMessage: &NackMessage{
			Header:         newHeader(MessageType_NACK, senderID, senderType, 0),
			TargetSenderId: targetSenderID,
			FromSeq:        fromSeq,
			ToSeq:          toSeq,
		},
	}
}

// UDP broadcast from new node to discover cluster
func NewJoinMsg(senderID string, senderType NodeType, address string) *JoinMsg {
	return &JoinMsg{
		JoinMessage: &JoinMessage{
			Header:  newHeader(MessageType_JOIN, senderID, senderType, 0),
			Address: address,
		},
	}
}

// UDP unicast from Leader to new node
func NewJoinResponseMsg(leaderID, leaderAddr, multicastGroup string, brokers []string) *JoinResponseMsg {
	return &JoinResponseMsg{
		JoinResponseMessage: &JoinResponseMessage{
			Header:          newHeader(MessageType_JOIN_RESPONSE, leaderID, NodeType_LEADER, 0),
			Success:         true,
			LeaderAddress:   leaderAddr,
			MulticastGroup:  multicastGroup,
			BrokerAddresses: brokers,
		},
	}
}

// TCP unicast from Broker to next Broker in logical ring
// ringParticipants ensures all nodes use the same filtered ring for consistency
func NewElectionMsg(senderID, candidateID string, electionID int64, phase ElectionMessage_Phase, ringParticipants []string) *ElectionMsg {
	return &ElectionMsg{
		ElectionMessage: &ElectionMessage{
			Header:           newHeader(MessageType_ELECTION, senderID, NodeType_BROKER, 0),
			CandidateId:      candidateID,
			ElectionId:       electionID,
			Phase:            phase,
			RingParticipants: ringParticipants,
		},
	}
}

// UDP multicast from Leader to Brokers, FIFO ordering
// View-synchronous: includes viewNumber and leaderID for consistency checks
func NewReplicateMsg(senderID string, stateSnapshot []byte, updateType string, seqNum int64, viewNumber int64, leaderID string) *ReplicateMsg {
	return &ReplicateMsg{
		ReplicateMessage: &ReplicateMessage{
			Header:        newHeader(MessageType_REPLICATE, senderID, NodeType_LEADER, seqNum),
			StateSnapshot: stateSnapshot,
			UpdateType:    updateType,
			ViewNumber:    viewNumber,
			LeaderId:      leaderID,
		},
	}
}

// TCP unicast from Follower to Leader acknowledging REPLICATE
// Passive replication: backups send acknowledgement (per slides)
func NewReplicateAckMsg(senderID string, ackedSeq, viewNumber int64, success bool, errorMessage string) *ReplicateAckMsg {
	return &ReplicateAckMsg{
		ReplicateAckMessage: &ReplicateAckMessage{
			Header:       newHeader(MessageType_REPLICATE_ACK, senderID, NodeType_BROKER, 0),
			AckedSeq:     ackedSeq,
			ViewNumber:   viewNumber,
			Success:      success,
			ErrorMessage: errorMessage,
		},
	}
}

// ============== View-Synchronous Recovery Message Factories ==============

// TCP unicast from new leader to followers during recovery
func NewStateExchangeMsg(senderID string, electionID, viewNumber int64) *StateExchangeMsg {
	return &StateExchangeMsg{
		StateExchangeMessage: &StateExchangeMessage{
			Header:     newHeader(MessageType_STATE_EXCHANGE, senderID, NodeType_LEADER, 0),
			ElectionId: electionID,
			ViewNumber: viewNumber,
		},
	}
}

// TCP unicast from followers to new leader during recovery
// topicLogs contains full log entries for view-synchronous UNION merge
func NewStateExchangeResponseMsg(senderID string, electionID, lastAppliedSeq int64, stateSnapshot []byte, hasCompleteState bool, logOffsets map[string]uint64, topicLogs []*TopicLogEntries) *StateExchangeResponseMsg {
	return &StateExchangeResponseMsg{
		StateExchangeResponseMessage: &StateExchangeResponseMessage{
			Header:           newHeader(MessageType_STATE_EXCHANGE_RESPONSE, senderID, NodeType_BROKER, 0),
			ElectionId:       electionID,
			LastAppliedSeq:   lastAppliedSeq,
			StateSnapshot:    stateSnapshot,
			HasCompleteState: hasCompleteState,
			LogOffsets:       logOffsets, // Deprecated but kept for compatibility
			TopicLogs:        topicLogs,
		},
	}
}

// TCP unicast from leader to all brokers to install new view
// mergedLogs contains union of all log entries for view-synchronous consistency
func NewViewInstallMsg(senderID string, viewNumber, agreedSeq int64, stateSnapshot []byte, memberIDs, memberAddresses []string, agreedLogOffsets map[string]uint64, mergedLogs []*TopicLogEntries) *ViewInstallMsg {
	return &ViewInstallMsg{
		ViewInstallMessage: &ViewInstallMessage{
			Header:           newHeader(MessageType_VIEW_INSTALL, senderID, NodeType_LEADER, 0),
			ViewNumber:       viewNumber,
			AgreedSeq:        agreedSeq,
			StateSnapshot:    stateSnapshot,
			MemberIds:        memberIDs,
			MemberAddresses:  memberAddresses,
			AgreedLogOffsets: agreedLogOffsets, // Deprecated but kept for compatibility
			MergedLogs:       mergedLogs,
		},
	}
}

// TCP unicast from brokers to leader acknowledging view installation
func NewViewInstallAckMsg(senderID string, viewNumber int64, success bool, errorMessage string) *ViewInstallAckMsg {
	return &ViewInstallAckMsg{
		ViewInstallAckMessage: &ViewInstallAckMessage{
			Header:       newHeader(MessageType_VIEW_INSTALL_ACK, senderID, NodeType_BROKER, 0),
			ViewNumber:   viewNumber,
			Success:      success,
			ErrorMessage: errorMessage,
		},
	}
}

// TCP unicast from Leader to Producer/Consumer for broker reassignment
func NewReassignBrokerMsg(senderID, clientID string, clientType NodeType, topic, newBrokerAddr, newBrokerID string) *ReassignBrokerMsg {
	return &ReassignBrokerMsg{
		ReassignBrokerMessage: &ReassignBrokerMessage{
			Header:           newHeader(MessageType_REASSIGN_BROKER, senderID, NodeType_LEADER, 0),
			ClientId:         clientID,
			ClientType:       clientType,
			Topic:            topic,
			NewBrokerAddress: newBrokerAddr,
			NewBrokerId:      newBrokerID,
		},
	}
}
