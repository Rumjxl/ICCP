#!/bin/bash
#1. Install Rust and Cargo commpiler
curl https://sh.rustup.rs -sSf | sh -s -- -y -v --default-toolchain nightly

#2. Compile CCP datapath: /pathto/ICCP-rust, cwd is /pathtp/ICCP-rust
make
sudo ./ccp_kernel_load --ipc=0

#3. Compile modified portus: cwd is /pathto/ICCP-rust
cd ./portus
cargo build

cd ..
cd ./lotus
cargo build

#4. Install cap`n proto
sudo apt install gcc g++ automake autoconf libtool
#cd /capnproto-c++
autoreconf -i
./configure
make -j6 check
sudo make install

#4.1 Install pycapnp in conda env
pip install pycapnp 

#5. Install iperf3 and mahimahi


