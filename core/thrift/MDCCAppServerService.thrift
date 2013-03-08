include "MDCCCommunicationService.thrift"
namespace java edu.ucsb.cs.mdcc.messaging

service MDCCAppServerService {
  bool ping(),
  MDCCCommunicationService.ReadValue read(1:string key),
  bool commit(1:string transactionId, 2:list<MDCCCommunicationService.Option> options)
}