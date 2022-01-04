#r "nuget: Akka.FSharp"

open Akka
open Akka.FSharp
open Akka.Actor
open System.Text
open System.Diagnostics
open System
open FSharp.Collections


// State of simulator actor
type SimulatorState = {
    NumOfNodes: int
    Topology: string
    Algorithm: string
}


// State of node actor
type NodeState = {
    Neighbors: Map<IActorRef,int>
    RumorFrequency: int
    S: float
    W: float
    SimulatorRef: IActorRef
    Active: bool
}


// Enumeration for messages sent between Simulator-worker
type Message =
    | Rumor
    | ComputePushSum of float * float
    | InitiateGossip
    | InitiatePushSum
    | ReportNeighbors of List<IActorRef>
    | AllRumorsReceived
    | Ack
    | Activate
    | Deactivate


// Declaration of constants
let tenPowerMinusTen : double = double (10.0)**(-10.0)

// Function to select a random element from a Set
let selectRandom(list: List<IActorRef>) =
    let testSeq = List.toSeq(list)
    let rnd = Random()
    testSeq |> Seq.sortBy (fun _ -> rnd.Next()) |> Seq.head


// Function to select a random element from map
let selectRandomFromMap(mp: Map<IActorRef, int>) =
    let keys = mp |> Map.toSeq |> Seq.map fst
    let rnd = Random()
    keys |> Seq.sortBy (fun _ -> rnd.Next()) |> Seq.head


// Function to increment Ack count for a node
let incrAck(mp:Map<IActorRef, int>, nbrRef:IActorRef) =
    mp 
        |> Map.change nbrRef (fun x -> 
            match x with
            | Some v -> 
                if (v+1)=10 then
                    //printfn "[NODE] %s is considered as failed." nbrRef.Path.Name
                    Some (-1)
                else
                    Some (v+1)
            | None -> None )

           
// Function to decrement Ack count for a node
let decrAck(mp:Map<IActorRef, int>, nbrRef:IActorRef) =
    mp 
        |> Map.change nbrRef (fun x -> 
            match x with
            | Some v -> Some (v-1)
            | None -> None )


// Function to get element from a Map
let getVal(mp:Map<IActorRef, int>, nbrRef:IActorRef) =
    let found = mp.TryFind nbrRef 
    match found with
    | Some v -> v
    | None -> -2


// Node Actor Definition
let node (i: int) (mailbox:Actor<_>) =
    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | Activate ->
            return! loop ({state with Active = true})
        | Deactivate ->
            return! loop ({state with Active = false})
        | Ack ->
            if getVal(state.Neighbors, sender) = -1 then
                //printfn "[NODE] %s is considered as alive again." sender.Path.Name
                return! loop ({state with Neighbors =  incrAck(state.Neighbors, sender)})    
            return! loop ({state with Neighbors =  decrAck(state.Neighbors, sender)})
        | Rumor ->
            if (sender <> state.SimulatorRef) && (sender <> mailbox.Self) then
                sender <! Ack

            if state.RumorFrequency + 1 = 10 then
                state.SimulatorRef <! AllRumorsReceived
            
            if state.RumorFrequency + 1 < 10 then
                let mutable destination = selectRandomFromMap(state.Neighbors)
                while getVal(state.Neighbors,destination) = -1 do
                    destination <- selectRandomFromMap(state.Neighbors)
                destination <! Rumor
                mailbox.Self <! Rumor
                return! loop ({state with RumorFrequency = state.RumorFrequency + 1; Neighbors =  incrAck(state.Neighbors, destination)})

            if mailbox.Self.Path.Name = sender.Path.Name then
                return! loop state
            
            return! loop ({state with RumorFrequency = state.RumorFrequency + 1})
        
        | InitiateGossip ->
            if (sender <> state.SimulatorRef) && (sender <> mailbox.Self) then
                sender <! Ack

            let mutable destination = selectRandomFromMap(state.Neighbors)
            while getVal(state.Neighbors,destination) = -1 do
                destination <- selectRandomFromMap(state.Neighbors)
            destination <! Rumor
            mailbox.Self <! Rumor

            return! loop ({state with RumorFrequency = state.RumorFrequency + 1; Neighbors =  incrAck(state.Neighbors, destination)})
        
        | ReportNeighbors (nbrs) ->
            let mutable nbrsMap:Map<IActorRef,int> = Map.empty
            for nbr in nbrs do
                nbrsMap <- nbrsMap.Add(nbr, 0)
            return! loop ({state with Neighbors = nbrsMap; SimulatorRef = sender})

        | InitiatePushSum ->
            if (sender <> state.SimulatorRef) && (sender <> mailbox.Self) then
                sender <! Ack

            let mutable destination = selectRandomFromMap(state.Neighbors)
            while getVal(state.Neighbors,destination) = -1 do
                destination <- selectRandomFromMap(state.Neighbors)
            destination <! ComputePushSum((state.S/2.0), (state.W/2.0))
            mailbox.Self <! InitiatePushSum
            return! loop {state with S=(state.S/2.0); W=(state.W/2.0); Neighbors =  incrAck(state.Neighbors, destination)}
        
        | ComputePushSum(s,w) ->
            if (sender <> state.SimulatorRef) && (sender <> mailbox.Self) then
                sender <! Ack

            let newS= state.S + s
            let newW = state.W + w
            let ratioDiff = state.S/state.W - newS/newW |> abs
            if state.RumorFrequency >= 3 then
                //let destination = selectRandom(state.Neighbors)
                //destination <! ComputePushSum(newS, newW)
                return! loop state
            else
                let mutable rf = 0
                if ratioDiff > tenPowerMinusTen then
                    rf <- 0
                else
                    rf <- state.RumorFrequency + 1
                
                if rf = 3 then
                    state.SimulatorRef <! AllRumorsReceived
                
                let mutable destination = selectRandomFromMap(state.Neighbors)
                while getVal(state.Neighbors,destination) = -1 do
                    destination <- selectRandomFromMap(state.Neighbors)
                destination <! ComputePushSum((newS/2.0), (newW/2.0))
                mailbox.Self <! InitiatePushSum
                return! loop {state with S=(newS/2.0); W=(newW/2.0); RumorFrequency=rf; Neighbors =  incrAck(state.Neighbors, destination)}
                
        | _ ->  failwith "[LOG] Unknown message."
    }

    loop {Neighbors=Map.empty; RumorFrequency=0; SimulatorRef=null; S=float(i); W=1.0; Active=true}


// Simulator Actor Definition
let simulator (numNodes: int, top: string, algo: string, numFails: int, systemRef: ActorSystem) (mailbox:Actor<_>) =
    // spawn all nodes
    let nodeList = [for i in 1..numNodes do (spawn systemRef ("Node"+string(i)) (node(i)))]
    let mutable nbrs:List<IActorRef> = List.empty
        
    // build and send topology
    if top = "full" then
        for i in 0..numNodes-1 do
            nbrs <- nodeList |> List.filter (fun x -> x <> nodeList.[i])
            nodeList.[i] <! ReportNeighbors(nbrs)
    
    if top = "line" then
        nbrs <- [nodeList.[1]]
        nodeList.[0] <! ReportNeighbors(nbrs)
        for i in 1..numNodes-2 do
            nbrs <- [nodeList.[i-1]; nodeList.[i+1]]
            nodeList.[i] <! ReportNeighbors(nbrs)
        nbrs <- [nodeList.[numNodes-2]]
        nodeList.[numNodes-1] <! ReportNeighbors(nbrs)

    if top = "3D" then
        let n = (int) (Math.Floor(Math.Pow((float)numNodes, ( (double)(1/3)) )) + (float)1);
        let n2 = (int) (float(n)**float(2))
        for i in 0..(numNodes-1) do
            nbrs <- List.empty
            if (i-1)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-1]]
            if (i+1)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+1]]
            if (i-n)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-n]]
            if (i+n)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+n]]
            if (i-n2)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-n2]]
            if (i+n2)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+n2]]
            nodeList.[i] <! ReportNeighbors(nbrs)
    
    if top = "imp3D" then
        let n = (int) (Math.Floor(Math.Pow((float)numNodes, ( (double)(1/3)) )) + (float)1);
        let n2 = (int) (float(n)**float(2))
        for i in 0..(numNodes-1) do
            nbrs <- List.empty
            if (i-1)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-1]]
            if (i+1)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+1]]
            if (i-n)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-n]]
            if (i+n)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+n]]
            if (i-n2)>=0 then
                nbrs <- List.append nbrs [nodeList.[i-n2]]
            if (i+n2)<numNodes then
                nbrs <- List.append nbrs [nodeList.[i+n2]]
            nbrs <- List.append nbrs [selectRandom(nodeList)]
            nodeList.[i] <! ReportNeighbors(nbrs)

    // initiate timer
    let timer = Stopwatch()

    // initiate
    let initNode = selectRandom(nodeList)
    if algo = "gossip" then
        printfn "[SIMULATOR][MAIN] Initiating Gossip Protocol with %d nodes in %s topology..." numNodes top
        timer.Start()
        initNode <! InitiateGossip
        let mutable failedSet: Set<IActorRef> = Set.empty
        let mutable i = 0
        while i<>numFails do
            let failNode = selectRandom(nodeList)
            if not (failedSet.Contains failNode) then
                //printfn "[SIMULATOR][INFO] Deactivating %s" failNode.Path.Name
                failNode <! Deactivate
                failedSet <- failedSet.Add failNode
                i <- i+1
            else
                i <- i-1
    else if algo = "pushsum" then
        printfn "[SIMULATOR][MAIN] Initiating Push-Sum Protocol with %d nodes in %s topology..." numNodes top
        timer.Start()
        initNode <! InitiatePushSum
    else
        printfn "[SIMULATOR][ERROR] Invalid algorithm."
        System.Environment.Exit(0)

    let rec loop state = actor {
        let! message = mailbox.Receive()
        let sender = mailbox.Sender()

        match message with
        | AllRumorsReceived ->
            //printfn "[SIMULATOR][INFO] Convergence occured at %s in %dms" sender.Path.Name timer.ElapsedMilliseconds
            if state.NumOfNodes-1 = 0 then
                printfn "[SIMULATOR][MAIN] All nodes converged in %dms" timer.ElapsedMilliseconds
                systemRef.Terminate()
                |> ignore
            return! loop ({state with NumOfNodes = state.NumOfNodes - 1})
        | _ ->  failwith "[SIMULATOR][ERROR] Unknown message."
    }

    loop {NumOfNodes=(numNodes-numFails); Topology=top; Algorithm=algo}


// Start of the program
let start (args: string[]) =
    let systemRef = ActorSystem.Create("System")

    spawn systemRef "simulator" (simulator(int(args.[1]), string(args.[2]), string(args.[3]), int(args.[4]), systemRef)) 
    |> ignore

    systemRef.WhenTerminated.Wait()
    

// Code starts here
start(fsi.CommandLineArgs)