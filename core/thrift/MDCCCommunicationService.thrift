namespace java edu.ucsb.cs.mdcc.messaging

struct BallotNumber {
  1:i64 number,
  2:string processId
}

struct ReadValue {
  1:i64 version
  2:binary value
}

service MDCCCommunicationService {

  bool ping(),
  
  bool prepare(1:string key, 2:BallotNumber ballot),

  bool accept(1:string transaction, 2:string key, 3:i64 oldVersion, 4:BallotNumber ballot, 5:binary newValue),
  
  void decide(1:string transaction, 2:bool commit)
  
  ReadValue read(1:string key)

}