extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema/")
        .file("schema/ccp.capnp")
        .run()
        .expect("capnp compiler command");
}
