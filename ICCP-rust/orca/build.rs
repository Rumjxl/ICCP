extern crate capnpc;

fn main() {
    println!("cargo:rerun-if-changed=schema/ccp.capnp");

    capnpc::CompilerCommand::new()
        .src_prefix("schema/")
        .file("schema/ccp.capnp")
        .run()
        .expect("capnp compiler command");
}
