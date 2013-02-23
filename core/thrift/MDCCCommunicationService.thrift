namespace java edu.ucsb.cs.mdcc.messaging

struct BallotNumber {
  1:i64 ballot,
  2:string processId
}

service MDCCCommunicationService {

  bool ping(),
  
  bool prepare(1:string object, 2:BallotNumber ballot),

  bool accept(1:string transaction, 2:string object, 3:i64 oldVersion, 4:BallotNumber ballot, 5:string newValue),
  
  void decide(1:string transaction, 2:bool commit)
  
  string get(1:string object)

}