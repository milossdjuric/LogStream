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
func NewHeartbeatMsg(senderID string, senderType NodeType, seqNum int64) *HeartbeatMsg {
	return &HeartbeatMsg{
		HeartbeatMessage: &HeartbeatMessage{
			Header: newHeader(MessageType_HEARTBEAT, senderID, senderType, seqNum),
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
func NewElectionMsg(senderID, candidateID string, electionID int64, phase ElectionMessage_Phase) *ElectionMsg {
	return &ElectionMsg{
		ElectionMessage: &ElectionMessage{
			Header:      newHeader(MessageType_ELECTION, senderID, NodeType_BROKER, 0),
			CandidateId: candidateID,
			ElectionId:  electionID,
			Phase:       phase,
		},
	}
}

// UDP multicast from Leader to Brokers, FIFO ordering
func NewReplicateMsg(senderID string, stateSnapshot []byte, updateType string, seqNum int64) *ReplicateMsg {
	return &ReplicateMsg{
		ReplicateMessage: &ReplicateMessage{
			Header:        newHeader(MessageType_REPLICATE, senderID, NodeType_LEADER, seqNum),
			StateSnapshot: stateSnapshot,
			UpdateType:    updateType,
		},
	}
}
