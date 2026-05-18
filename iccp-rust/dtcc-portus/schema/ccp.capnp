@0x97d8c9b4831fdcbb;
const qux :UInt32 = 123;

struct Observation {
    bytesAcked         @0   :UInt64;
    loss               @1   :UInt64;
    rtt                @2  :UInt64;
    rttvar             @3  :UInt64;
    castate            @4  :UInt64;
    minrtt             @5  :UInt64;
    timeDelta          @6  :UInt64;
    sndMss             @7  :UInt64;
    delivered         @8  :UInt64;
    deliveryRate       @9  :UInt64;
    unacked            @10   :UInt64;
    duration            @11  :UInt64;
    sndCwnd            @12  :UInt64;
    bytesSent            @13   :UInt64;
    connectionId       @14  :UInt64;
    rpcSendMonoNs      @15  :UInt64;
    isNewFlow          @16  :Bool;
}

struct Action {
    rate @0 :UInt32;
    cwnd @1 :UInt32;
}

interface RLAgent {
    getAction @0 (observation :Observation) -> (action :Action);
}
