//! 
//! client.rs
//! Implementation of 2PC client
//! 
extern crate log;
extern crate stderrlog;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::atomic::{AtomicI32, AtomicBool, Ordering};
use std::sync::{Arc};
use std::time::Duration;
use std::thread;
use std::collections::HashMap;
use message;
use message::MessageType;
use message::RequestStatus;

// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and 
// primitives for communicating with 
// the coordinator
#[derive(Debug)]
pub struct Client {    
    pub id: i32,
    pub tx: Sender<message::ProtocolMessage>,
    pub rx: Receiver<message::ProtocolMessage>,
    pub running: Arc<AtomicBool>,
    pub str: String,
    success: usize,
    failed: usize,
    unknown: usize,
    // ...
}

///
/// client implementation
/// Required: 
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown 
/// 3. pub fn protocol(&mut self, n_requests: i32) -- implements client side protocol
///
impl Client {

    /// 
    /// new()
    /// 
    /// Return a new client, ready to run the 2PC protocol
    /// with the coordinator. 
    pub fn new(i: i32,
               is: String,
               tx: Sender<message::ProtocolMessage>,
               rx: Receiver<message::ProtocolMessage>,
               r: Arc<AtomicBool>) -> Client {
        Client {
            id: i,
            tx: tx,
            rx: rx,
            running: r,
            str: is,
            success: 0,
            failed: 0,
            unknown: 0,
            // ...
        }   
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("Client_{} waiting for exit signal", self.id);

        while self.running.load(Ordering::SeqCst) {}
        // println!("Client_{} received exit signal", self.id);

        trace!("Client_{} exiting", self.id);
    }

    /// 
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    /// 
    pub fn send_next_operation(&mut self) {

        trace!("Client_{}::send_next_operation", self.id);

        // create a new request with a unique TXID.         
        let request_no: i32 = TXID_COUNTER.load(Ordering::SeqCst);
        let txid = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);

        info!("Client {} request({})->txid:{} called", self.id, request_no, txid);
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest, 
                                                    txid, 
                                                    format!("Client_{}", self.id), 
                                                    request_no);

        info!("client {} calling send...", self.id);

        // send the request to the coordinator
        self.tx.send(pm).unwrap();
        // println!("Client_{} request({})->txid:{} sent", self.id, request_no, txid);

        trace!("Client_{}::exit send_next_operation", self.id);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the 
    /// last issued request. Note that we assume the coordinator does 
    /// not fail in this simulation
    /// 
    pub fn recv_result(&mut self) {
        trace!("Client_{}::recv_result", self.id);

        // let pm = self.rx.recv().unwrap();
        // let mut result: RequestStatus = RequestStatus::Unknown;
        let pm = self.rx.recv_timeout(Duration::from_millis(500));
        let mut result: RequestStatus = RequestStatus::Unknown;
        if pm.is_err() {
            // println!("request for client {} timed out", self.id);
            self.unknown += 1;
            return;
        }
        let pm = pm.unwrap();

        match pm.mtype {
            MessageType::ClientResultCommit => {
                // println!("Client {} request({})->txid:{} committed", self.id, pm.opid, pm.txid);
                result = RequestStatus::Committed;
                self.success += 1;
            },
            MessageType::ClientResultAbort => {
                // println!("Client {} request({})->txid:{} aborted", self.id, pm.opid, pm.txid);
                result = RequestStatus::Aborted;
                self.failed += 1;
            },
            MessageType::CoordinatorExit => {
                // println!("Client {} request({})->txid:{} coordinator exiting", self.id, pm.opid, pm.txid);
                // self.running.store(false, Ordering::SeqCst);
            },
            _ => {
                // panic!("Client {} request({})->txid:{} invalid response type", self.id, pm.opid, pm.txid);
                // println!("Client {} request({})->txid:{} invalid response type", self.id, pm.opid, pm.txid);
                // self.unknown += 1;
            }
        }

        trace!("Client_{}::exit recv_result", self.id);
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this client before exiting. 
    /// 
    pub fn report_status(&mut self) {
        let successful_ops: usize = self.success;
        let failed_ops: usize = self.failed;
        let unknown_ops: usize = self.unknown;
        println!("Client_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// If the simulation ends early, stop issuing requests
    pub fn protocol(&mut self, n_requests: i32) {

        // run the 2PC protocol for each of n_requests
        let mut i = 0;
        while self.running.load(Ordering::SeqCst) && i < n_requests {
            // send a new operation to the coordinator
            self.send_next_operation();
            // wait for the result from the coordinator
            self.recv_result();

            i += 1;
        }

        // wait for signal to exit
        // and then report status
        self.wait_for_exit_signal();
        // self.report_status();
    }
}
