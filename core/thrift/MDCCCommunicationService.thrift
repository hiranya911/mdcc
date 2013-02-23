namespace java edu.ucsb.cs.mdcc.messaging

struct RecordVersion {
  1:i64 ballot,
  2:string processId
}

service MDCCCommunicationService {

  bool ping(),
  
  bool prepare(1:string object, 2:RecordVersion version),

  bool accept(1:string transaction, 2:string object, 3:RecordVersion oldVersion, 4:string processId, 5:string newValue),
  
  void decide(1:string transaction, 2:bool commit)
  
  string get(1:string object)

}