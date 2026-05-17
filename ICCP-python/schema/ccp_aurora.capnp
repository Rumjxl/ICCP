@0x97d8c9b4831fdcbb;
const qux :UInt32 = 123;

struct Observation {
    rttInflation    @0   :UInt32;
    rtt             @1   :UInt32;
    minrtt          @2   :UInt32;
    bytesSent       @3   :UInt32;
    bytesAcked      @4   :UInt32;
    sndDuration     @5   :UInt64;
    rcvDuration     @6   :UInt64;
    loss            @7   :UInt32;
    sndCwnd         @8   :UInt32;
    sndMss          @9   :UInt32;
}

struct Action {
    rate @0 :UInt32;
    cwnd @1 :UInt32;
}

interface RLAgent {
    getAction @0 (observation :Observation) -> (action :Action);
}


