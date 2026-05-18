@0x97d8c9b4831fdcbb;
const qux :UInt32 = 123;

struct Observation {
    avgrtt         @0   :UInt32;
    deliveryRate   @1   :UInt64;
    cnt            @2  :UInt32;
    timeDelta      @3  :UInt64;
    sndCwnd        @4  :UInt32;
    pacingRate     @5  :UInt64;
    loss           @6  :UInt32;
    srtt           @7  :UInt32;
    minrtt         @8  :UInt32;
    connectionId       @9  :UInt64;
    rpcSendMonoNs      @10 :UInt64;
}

struct Action {
    rate @0 :UInt32;
    cwnd @1 :UInt32;
}

interface RLAgent {
    getAction @0 (observation :Observation) -> (action :Action);
}
