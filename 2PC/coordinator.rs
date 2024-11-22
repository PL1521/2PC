//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use coordinator::rand::prelude::*;use std::thread;
use std::sync::{Arc};
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32};
use std::sync::atomic::{AtomicBool, Ordering};
use message::ProtocolMessage;
use message::MessageType;
use message::RequestStatus;
use message;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
/// 
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {    
    Quiescent,
    WaitingForParticipants,
    SendingRequest,
    WaitingForResponses,
}

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    log: oplog::OpLog,
    msg_success_prob: f64,
    running: Arc<AtomicBool>,
    pub client_channels: HashMap<String, Sender<ProtocolMessage>>,
    pub participant_channels: HashMap<String, Sender<ProtocolMessage>>,
    pub from_participants: Receiver<ProtocolMessage>,
    pub from_clients: Receiver<ProtocolMessage>,
    commits: usize,
    aborts: usize,
    unknowns: usize,
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
/// 
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    /// 
    /// <params>
    ///     logpath: directory for log files --> create a new log there. 
    ///     r: atomic bool --> still running?
    ///     msg_success_prob --> probability sends succeed
    ///
    pub fn new(
        logpath: String, 
        r: &Arc<AtomicBool>, 
        msg_success_prob: f64) -> Coordinator {

        // println!("Coordinator failure probability: {}", 1.0 - msg_success_prob);

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(logpath),
            msg_success_prob: msg_success_prob,
            running: r.clone(),
            client_channels: HashMap::new(),
            participant_channels: HashMap::new(),
            from_participants: mpsc::channel::<ProtocolMessage>().1,
            from_clients: mpsc::channel::<ProtocolMessage>().1,
            commits: 0,
            aborts: 0,
            unknowns: 0,
        }
    }

    /// 
    /// participant_join()
    /// handle the addition of a new participant
    /// Keep track of channels involved
    /// 
    pub fn participant_join(
        &mut self,
        name: &String,
    ) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
        assert!(self.state == CoordinatorState::Quiescent);
        let (send, recv) = mpsc::channel();
        self.participant_channels.insert(name.clone(), send.clone());
        (send, recv)
    }

    ///
    /// client_join()
    /// handle the addition of a new client
    /// Keep track of channels involved
    ///
    pub fn client_join(&mut self, name: &String) {
        assert!(self.state == CoordinatorState::Quiescent);
        self.client_channels.insert(name.clone(), mpsc::channel().0);
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    ///
    pub fn send(&self, sender: &Sender<ProtocolMessage>, pm: ProtocolMessage) -> bool {

        let mut x: f64 = random();
        let mut result: bool = true;

        match pm.mtype {
            MessageType::CoordinatorPropose => {
                x = 0.0;
            },
            MessageType::CoordinatorCommit => {
                x = 0.0;
            },
            MessageType::CoordinatorAbort => {
                x = 0.0;
            },
            _ => {
                // panic!("coordinator::send: invalid message type");
            }
        }


        if x < self.msg_success_prob {

            // send the message!
            // let pcopy = pm.clone();
            sender.send(pm).unwrap();
            // println!("coordinator::send: {:?}", pcopy);

        } else {
            
            // don't send anything!
            // (simulates failure)
            result = false;
            // println!("coordinator::send: dropped message to {}", pm.senderid);
        }
        result
    }     

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&mut self) -> Option<ProtocolMessage> {
        let mut result = Option::None;
        assert!(self.state == CoordinatorState::Quiescent);        
        trace!("coordinator::recv_request...");

        // let pm = self.from_clients.recv().unwrap();
        // result = Some(pm);
        // // println!("coordinator::recv_request: {:?}", result);
        if let Ok(request) = self.from_clients.recv() {
            result = Some(request);
        }

        trace!("leaving coordinator::recv_request");
        result
    }        

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {
        let successful_ops: usize = self.commits;
        let failed_ops: usize = self.aborts;
        let unknown_ops: usize = self.unknowns; 
        println!("coordinator:\tC:{}\tA:{}\tU:{}", successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// If the simulation ends early, stop handling requests
    /// 
    pub fn protocol(&mut self) {
        let mut it = 0;

        while self.running.load(Ordering::SeqCst) {
            // let start = std::time::Instant::now();
            let pm = self.recv_request();
            if pm.is_none() {
                // println!("coordinator::protocol: no request received");
                continue;
            }
            
            let pm = pm.unwrap();
            // println!("coordinator::protocol: received request {:?}", pm);
            self.state = CoordinatorState::WaitingForParticipants;

            // send message to all participants
            let mut participants: Vec<String> = Vec::new();
            for (k, _) in &self.participant_channels {
                participants.push(k.clone());
            }
            let mut responses: Vec<ProtocolMessage> = Vec::new();
            // it += 1;
            // println!("coordinator::protocol: proposing round {} for txid {}", it, pm.txid);
            for p in &participants {
                let send_channel = self.participant_channels.get(p).unwrap();
                let pm = ProtocolMessage::instantiate(
                    MessageType::CoordinatorPropose,
                    pm.uid,
                    pm.txid,
                    pm.senderid.clone(),
                    pm.opid,
                );
                // println!("coordinator::protocol: sending \"request\" to participant node {}", p);
                // self.send(send_channel, pm);
                // let tmp = pm.clone();
                // self.send(send_channel, pm);
                let tmp = pm.clone();
                if !self.send(send_channel, pm) {
                    if tmp.mtype == MessageType::CoordinatorPropose {
                        println!("coordinator::propose: dropped proposal to participant {}", p.clone());
                    }
                }
            }

            // wait for responses from all participants
            self.state = CoordinatorState::WaitingForResponses;
            for _ in 0..participants.len() {
                // let pm = self.from_participants.recv().unwrap();
                // specify timeout
                let pm = self.from_participants.recv_timeout(Duration::from_millis(10));
                if pm.is_err() {
                    // println!("coordinator::protocol: timeout");
                    // add in a dummy response
                    let tmp = ProtocolMessage::instantiate(
                        MessageType::ParticipantVoteAbort,
                        -1,
                        -1,
                        String::from("dummy"),
                        -1,
                    );
                    responses.push(tmp);
                    // self.unknowns += 1;
                    continue;
                }
                let pm = pm.unwrap();
                // println!("coordinator::protocol: received response {:?}", pm);
                responses.push(pm);
            }
            

            // check responses
            let mut aborts = 0;
            let mut commits = 0;
            let mut abort = false;
            for pm in &responses {
                match pm.mtype {
                    MessageType::ParticipantVoteCommit => {
                        commits += 1;
                    },
                    MessageType::ParticipantVoteAbort => {
                        aborts += 1;
                        abort = true;
                    },
                    _ => {
                        panic!("coordinator::protocol: invalid response type");
                    }
                }
            }

            // send message to all participants
            for p in &participants {
                let send_channel = self.participant_channels.get(p).unwrap();
                let pm = ProtocolMessage::instantiate(
                    if abort { MessageType::CoordinatorAbort } else { MessageType::CoordinatorCommit },
                    pm.uid,
                    pm.txid,
                    pm.senderid.clone(),
                    pm.opid,
                );
                // if abort {
                //     // println!("coordinator::protocol: sending \"abort\" to participant node {}", p);
                // } else {
                //     // println!("coordinator::protocol: sending \"commit\" to participant node {}", p);
                // }
                // self.send(send_channel, pm);
                let tmp = pm.clone();
                if !self.send(send_channel, pm) {
                    if tmp.mtype == MessageType::CoordinatorAbort || tmp.mtype == MessageType::CoordinatorCommit {
                        println!("coordinator::send: dropped message to participant {}", p.clone());
                    }
                }
            }
            // println!("coordinator::protocol: aborts: {}, commits: {}, final decision: {}", aborts, commits, if abort { "abort" } else { "commit" });
            if abort {
                self.aborts += 1;
            } else {
                self.commits += 1;
            }

            // notify client
            self.state = CoordinatorState::Quiescent;

            let client = pm.senderid.clone();
            // send message back to client
            let send_channel = self.client_channels.get(&client).unwrap();
            let pm = ProtocolMessage::instantiate(
                if abort { MessageType::ClientResultAbort } else { MessageType::ClientResultCommit },
                pm.uid,
                pm.txid,
                pm.senderid.clone(),
                pm.opid,
            );
            // println!("coordinator::protocol: sending \"commit\" to client node {}", client);
            let res_type = if abort { MessageType::CoordinatorAbort } else { MessageType::CoordinatorCommit };
            self.log.append(pm.mtype, pm.opid, pm.senderid.clone(), pm.opid);
            self.log.append(res_type, pm.opid, pm.senderid.clone(), pm.opid);
            self.send(send_channel, pm);
            // let end = std::time::Instant::now();
            // let elapsed = end.duration_since(start);
            // println!("coordinator::protocol: elapsed time: {:?}", elapsed);
        }

        // println!("coordinator::protocol: exiting");

        self.report_status();                        
    }
}
