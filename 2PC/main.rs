#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
use std::fmt::format;
use std::thread;
use std::thread::JoinHandle;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use coordinator::Coordinator;
use participant::Participant;
use client::Client;
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};

///
/// register_clients()
/// 
/// The coordinator needs to know about all clients. 
/// This function should create clients and use some communication 
/// primitive to ensure the coordinator and clients are aware of 
/// each other and able to exchange messages. Starting threads to run the
/// client protocol should be deferred until after all the communication 
/// structures are created. 
/// 
/// Note: Probably want to look at rust's mpsc::channel or crossbeam 
///       channels to set up communication. Communication in 2PC 
///       is duplex!
/// 
/// <params>
///     coordinator: the coordinator!
///     n_clients: number of clients to create and register
///     logpathbase: each participant, client, and the coordinator 
///         needs to maintain its own operation and commit log. 
///         The project checker assumes a specific directory structure 
///         for files backing these logs. Concretely, participant log files 
///         will be expected to be produced in:
///            logpathbase/client_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///
fn register_clients(
    coordinator: &mut Coordinator,
    n_clients: i32,
    running: &Arc<AtomicBool>) -> Vec<Client> {

    let mut clients = vec![];
    // register clients with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.


    // 1 mpsc channel connecting all clients to the coordinator
    let (to_coordinator, from_clients) = std::sync::mpsc::channel::<message::ProtocolMessage>();
    coordinator.from_clients = from_clients;
    for i in 0..n_clients {
        let (to_client, from_coordinator) = std::sync::mpsc::channel();
        let client = Client::new(i, format!("Client_{}", i), to_coordinator.clone(), from_coordinator, running.clone());
        clients.push(client);
        coordinator.client_channels.insert(format!("Client_{}", i), to_client);
    }

    // println!("Coordinator: {} clients registered", n_clients);
    // println!("Coordinator mappping: {:?}", coordinator.client_channels);
    
    clients
}

/// 
/// register_participants()
/// 
/// The coordinator needs to know about all participants. 
/// This function should create participants and use some communication 
/// primitive to ensure the coordinator and participants are aware of 
/// each other and able to exchange messages. Starting threads to run the
/// participant protocol should be deferred until after all the communication 
/// structures are created. 
/// 
/// NOTE: Probably want to look at rust's mpsc::channel or crossbeam 
///       channels to set up communication. Note that communication in 2PC 
///       is duplex!
/// 
/// <params>
///     coordinator: the coordinator!
///     n_participants: number of participants to create an register
///     logpathbase: each participant, client, and the coordinator 
///         needs to maintain its own operation and commit log. 
///         The project checker assumes a specific directory structure 
///         for files backing these logs. Concretely, participant log files 
///         will be expected to be produced in:
///            logpathbase/participant_<num>.log
///     running: atomic bool indicating whether the simulation is still running
///     success_prob_op: [0.0..1.0] probability that operations succeed.
///     success_prob_msg: [0.0..1.0] probability that sends succeed.
///
fn register_participants(
    coordinator: &mut Coordinator,
    n_participants: i32,
    logpathbase: &String,
    running: &Arc<AtomicBool>, 
    success_prob_op: f64,
    success_prob_msg: f64) -> Vec<Participant> {

    let mut participants = vec![];
    // register participants with coordinator (set up communication channels and sync objects)
    // add client to the vector and return the vector.
    let (to_coordinator, from_participants) = std::sync::mpsc::channel::<message::ProtocolMessage>();
    coordinator.from_participants = from_participants;
    for i in 0..n_participants {
        let (to_participant, from_coordinator) = std::sync::mpsc::channel();
        let participant = Participant::new(i, 
                                                        format!("Participant_{}", i), 
                                                        to_coordinator.clone(), 
                                                        from_coordinator, 
                                                        format!("{}/participant_{}.log", logpathbase, i), 
                                                        running, 
                                                        success_prob_op, 
                                                        success_prob_msg);
        participants.push(participant);
        coordinator.participant_channels.insert(format!("Participant_{}", i), to_participant);
    }

    // println!("Coordinator: {} participants registered", n_participants);
    // println!("Coordinator mappping: {:?}", coordinator.participant_channels);

    participants
}

///
/// launch_clients()
/// 
/// create a thread per client to run the client
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Client::protocol(...). Telling the client
/// how many requests to send is probably a good idea. :-)
/// 
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector 
///    to return wait handles to the caller
///
fn launch_clients(
    clients: Vec<Client>,
    n_requests: i32,
    handles: &mut Vec<JoinHandle<()>>) {

    // do something to create threads for client 'processes'
    // the mutable handles parameter allows you to return 
    // more than one wait handle to the caller to join on. 
    for mut client in clients {
        let handle = thread::spawn(move || {
            client.protocol(n_requests);
        });
        handles.push(handle);
    }
}

///
/// launch_participants()
/// 
/// create a thread per participant to run the participant 
/// part of the 2PC protocol. Somewhere in each of the threads created
/// here, there should be a call to Participant::participate(...).
/// 
/// <params>
/// participants: a vector of Participant structs
/// handles: (optional depending on design) -- a mutable vector 
///    to return wait handles to the caller
///
fn launch_participants(
    participants: Vec<Participant>,
    handles: &mut Vec<JoinHandle<()>>) {

    // do something to create threads for participant 'processes'
    // the mutable handles parameter allows you to return 
    // more than one wait handle to the caller to join on. 
    for mut participant in participants {
        let handle = thread::spawn(move || {
            participant.protocol();
        });
        handles.push(handle);
    }
}

/// 
/// run()
/// opts: an options structure describing mode and parameters
/// 
/// 0. install a signal handler that manages a global atomic boolean flag
/// 1. creates a new coordinator
/// 2. creates new clients and registers them with the coordinator
/// 3. creates new participants and registers them with coordinator
/// 4. launches participants in their own threads
/// 5. launches clients in their own threads
/// 6. creates a thread to run the coordinator protocol
/// 
fn run(opts: & tpcoptions::TPCOptions) {

    // vector for wait handles, allowing us to 
    // wait for client, participant, and coordinator 
    // threads to join.
    let mut handles: Vec<JoinHandle<()>> = vec![];    

    // create an atomic bool object and a signal handler
    // that sets it. this allows us to inform clients and 
    // participants that we are exiting the simulation 
    // by pressing "control-C", which will set the running 
    // flag to false. 
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    ctrlc::set_handler(move || {
        println!("CTRL-C!");
        r.store(false, Ordering::SeqCst); 
    }).expect("Error setting signal handler!");

    // create a coordinator, create and register clients and participants
    // launch threads for all, and wait on handles. 
    let cpath = format!("{}//{}", opts.logpath, "coordinator.log");
    let mut coordinator: Coordinator;  
    let clients: Vec<Client>; 
    let participants: Vec<Participant>;

    // wait for clients, participants, and coordinator here...
    coordinator = Coordinator::new(cpath, &running, opts.success_probability_msg);
    clients = register_clients(&mut coordinator, opts.num_clients, &running);
    participants = register_participants(
                        &mut coordinator, 
                        opts.num_participants, 
                        &opts.logpath, 
                        &running, 
                        opts.success_probability_ops, 
                        opts.success_probability_msg);
    // let handle = thread::spawn(move || {
    //     coordinator.protocol();
    // });
    let handle = thread::Builder::new().name("coordinator".to_string()).spawn(move || {
        coordinator.protocol();
    }).unwrap();
    handles.push(handle);
    launch_clients(clients, opts.num_requests, &mut handles);
    launch_participants(participants, &mut handles);
    
    // wait for all threads to exit
    for i in handles {
        i.join().unwrap();
    }
}

use std::env;
///
/// main()
/// 
fn main() {
    env::set_var("RUST_BACKTRACE", "1");
    
    let opts = tpcoptions::TPCOptions::new();
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();

    match opts.mode.as_ref() {

        "run" => run(&opts),
        "check" => checker::check_last_run(opts.num_clients, 
                                        opts.num_requests, 
                                        opts.num_participants, 
                                        &opts.logpath.to_string()),
        _ => panic!("unknown mode"),
    }
}
