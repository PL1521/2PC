//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use participant::rand::prelude::*;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::sync::atomic::{AtomicI32};
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use std::collections::HashMap;
use std::thread;
use oplog;

/// 
/// ParticipantState
/// enum for participant 2PC state machine
/// 
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {    
    Quiescent,          
    WaitingForVoteRequest,
    WaitingForVote,
}

///
/// Participant
/// structure for maintaining per-participant state 
/// and communication/synchronization objects to/from coordinator
/// 
#[derive(Debug)]
pub struct Participant {    
    id: i32,
    state: ParticipantState,
    log: oplog::OpLog,
    op_success_prob: f64,
    msg_success_prob: f64,
    pub tx: Sender<ProtocolMessage>,
    pub rx: Receiver<ProtocolMessage>,
    pub is: String,
    pub running: Arc<AtomicBool>,
    commits: usize,
    aborts: usize,
    unknowns: usize,
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {

    /// 
    /// new()
    /// 
    /// Return a new participant, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// NOTE: Pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// NOTE: Pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: i32, is: String, 
        tx: Sender<ProtocolMessage>, 
        rx: Receiver<ProtocolMessage>, 
        logpath: String,
        r: &Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64) -> Participant {

        Participant {
            id: i,
            log: oplog::OpLog::new(logpath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            state: ParticipantState::Quiescent,
            tx: tx,
            rx: rx,
            is: is,
            running: r.clone(),
            commits: 0,
            aborts: 0,
            unknowns: 0,
        }   
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    pub fn send(&mut self, pm: ProtocolMessage) -> bool {
        let result: bool;

        result = self.tx.send(pm).is_ok();

        result
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    pub fn send_unreliable(&mut self, pm: ProtocolMessage) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send(pm);
        } else {
            result = false;
        }
        result
    }    

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    pub fn perform_operation(&mut self, request: &Option<ProtocolMessage>) -> bool {

        trace!("participant::perform_operation");

        let mut result: RequestStatus = RequestStatus::Unknown;

        let x: f64 = random();
        if x > self.op_success_prob {
            
            // Fail the request
            result = RequestStatus::Aborted;
            let mut pm = request.clone().unwrap();
            self.log.append(MessageType::ParticipantVoteAbort, pm.txid, self.is.clone(), pm.opid);

        } else {

            // Request succeeds!
            result = RequestStatus::Committed;
            let mut pm = request.clone().unwrap();
            self.log.append(MessageType::ParticipantVoteCommit, pm.txid, self.is.clone(), pm.opid);
            

        }

        trace!("exit participant::perform_operation");
        result == RequestStatus::Committed
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // Maintain actual stats
        let global_successful_ops: usize = self.commits;
        let global_failed_ops: usize = self.aborts;
        let global_unknown_ops: usize = self.unknowns;
        println!("participant_{}:\tC:{}\tA:{}\tU:{}\t", self.id, global_successful_ops, global_failed_ops, global_unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("participant_{} waiting for exit signal", self.id);

        while self.running.load(Ordering::SeqCst) {}
        // println!("participant_{} received exit signal", self.id);

        trace!("participant_{} exiting", self.id);
    }    

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// If the simulation ends early, stop handling requests
    /// 
    pub fn protocol(&mut self) {
        
        trace!("Participant_{}::protocol", self.id);

        while self.running.load(Ordering::SeqCst) {
            // get proposal from coordinator
            let mut proposal: Option<ProtocolMessage> = None;
            if let Ok(pm) = self.rx.recv_timeout(Duration::from_millis(1000)) {
                proposal = Some(pm);
            }

            if proposal.is_none() {
                // println!("Participant_{}::protocol: no proposal received", self.id);
                continue;
            }

            let mut pm = proposal.clone().unwrap();
            // println!("Participant_{}::protocol: received proposal {:?}", self.id, pm);

            // perform the operation
            let result = self.perform_operation(&proposal);
            // println!("Participant_{}::protocol: performed operation {:?}", self.id, result);

            // send the result to the coordinator
            pm.mtype = if result { MessageType::ParticipantVoteCommit } else { MessageType::ParticipantVoteAbort };
            if self.send_unreliable(pm) {
                // println!("Participant_{}::protocol: sent result {:?}", self.id, result);
            } else {
                // println!("Participant_{}::protocol: failed to send result {:?}", self.id, result);
            }

            // wait for the final decision from the coordinator
            let mut decision: Option<ProtocolMessage> = None;
            if let Ok(pm) = self.rx.recv() {
                decision = Some(pm);
            }

            if decision.is_none() {
                // println!("Participant_{}::protocol: no decision received", self.id);
                continue;
            }

            let mut pm = decision.clone().unwrap();
            match pm.mtype {
                MessageType::CoordinatorCommit => {
                    // println!("Participant_{}::protocol: received decision {:?}", self.id, pm.mtype);
                    self.log.append(pm.mtype, pm.opid, pm.senderid, pm.opid);
                    self.commits += 1;
                },
                MessageType::CoordinatorAbort => {
                    // println!("Participant_{}::protocol: received decision {:?}", self.id, pm.mtype);
                    self.log.append(pm.mtype, pm.opid, pm.senderid, pm.opid);
                    self.aborts += 1;
                },
                _ => {
                    // println!("Participant_{}::protocol: received decision {:?}", self.id, pm.mtype);
                    self.unknowns += 1;
                }
            }
        }

        self.wait_for_exit_signal();
        self.report_status();
    }
}
