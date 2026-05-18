# ICCP

ICCP is a flexible framework to integrate both heruistic and learning-based congestion-control algorithms. It combines
a Rust CCP datapath/client runtime with a Python model-inference agent. The Rust
side collects TCP observations and applies congestion-control updates; the
Python side loads a policy model, serves a Cap'n Proto RPC endpoint, and returns
congestion-window / pacing-rate decisions.

This repository currently focuses on three model families:

- `dt`: DTCC, based on Decision Transformer
- `sage`: SAGE policy
- `orca`: ORCA online RL policy

## Repository Layout

```text
ICCP/
├── iccp-python/                         # Python model agent and RPC server
│   ├── dtcc.py                          # Main Python entry point
│   ├── dtcc_agent.py                    # DTCC / Decision Transformer agent
│   ├── sage_agent.py                    # SAGE agent wrapper
│   ├── orca_agent.py                    # ORCA agent wrapper
│   ├── config-rl-eval.yaml
│   ├── config-sage-eval.yaml
│   ├── config-orca-eval.yaml
│   ├── decision_transformer/
│   │   ├── envs/
│   │   │   ├── cc.py                    # RPC server + Rust client launcher
│   │   │   ├── cc_standalone.py         # RPC server only
│   │   │   └── timestep.py
│   │   └── models/
│   │       ├── decision_transformer.py
│   │       ├── model.py
│   │       └── trajectory_gpt2.py
│   ├── sage_special/                    # SAGE network code
│   ├── orca_special/                    # ORCA network code
│   ├── schema/                          # Python-side Cap'n Proto schemas
│   ├── utils/
│   ├── dt_ckpt/                         # DTCC checkpoint
│   ├── sage_ckpt/                       # SAGE checkpoint
│   └── orca_ckpt/                       # ORCA checkpoint
│
└── iccp-rust/                           # Rust CCP datapath and clients
    ├── dtcc/                            # DTCC lotus client
    ├── dtcc-portus/                     # DTCC portus client
    ├── orca/                            # ORCA lotus client
    ├── orca-portus/                     # ORCA portus client
    ├── iccp/                            # Hybrid ICCP runtime
    ├── bbr/
    ├── bbr-portus/
    ├── lotus/
    ├── portus/
    ├── libccp/
    ├── libccp-rust/
    ├── Makefile                         # Kernel datapath build
    ├── ccp_kernel_load
    └── ccp_kernel_unload
```

## Architecture

ICCP runs as two cooperating processes:

1. The Python agent loads one model checkpoint and starts a Cap'n Proto RPC
   server.
2. The Rust CCP client connects to the Python RPC server, receives per-flow TCP
   measurements from the kernel datapath, sends observations to Python, and
   applies returned actions to the flow.

The default RPC address is:

```text
127.0.0.1:4826
```

Model-to-client mapping:

| Python model | Lotus client | Portus client | RPC schema |
| --- | --- | --- | --- |
| `dt` | `iccp-rust/dtcc/target/debug/dtcc` | `iccp-rust/dtcc-portus/target/debug/dtcc` | `ccp_dtcc.capnp` |
| `sage` | `iccp-rust/dtcc/target/debug/dtcc` | `iccp-rust/dtcc-portus/target/debug/dtcc` | `ccp_dtcc.capnp` |
| `orca` | `iccp-rust/orca/target/debug/orca` | `iccp-rust/orca-portus/target/debug/orca` | `ccp_orca.capnp` |
| hybrid | `iccp-rust/iccp/target/debug/iccp` | - | `ccp_dtcc.capnp` |

## Requirements

System requirements:

- Linux with CCP-compatible kernel support
- Rust nightly toolchain
- GCC / G++ / build-essential
- Linux kernel headers for the running kernel
- Cap'n Proto compiler and runtime
- `iperf3`
- Mahimahi, if running trace-based experiments
- `sudo` privileges for loading kernel modules and starting CCP clients

Python requirements:

- Python 3.8 is recommended
- PyTorch
- TensorFlow
- `numpy`
- `pyyaml`
- `gym`
- `transformers`
- `pycapnp`
- `psutil`
- `sysv_ipc`
- `dm-acme`
- `dm-sonnet`

`iccp-python/requirements.txt` was exported from a local environment and may
include extra transitive packages. For a cleaner release environment, maintain a
curated `environment.yml` or `requirements.txt`.

## Path Assumptions

Run Python commands from `iccp-python/`. The Python RPC code loads schemas from
the local `schema/` directory and starts Rust clients from the sibling Rust tree:

```text
../iccp-rust/
```

Because Linux paths are case-sensitive, make sure the launcher path in
`iccp-python/decision_transformer/envs/cc.py` and
`iccp-python/decision_transformer/envs/cc_standalone.py` matches the directory
name `iccp-rust`.

## Build Rust Components

Install Rust nightly:

```bash
curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain nightly
source "$HOME/.cargo/env"
```

Build the CCP kernel datapath:

```bash
cd ICCP/iccp-rust
make
```

Load the CCP kernel module:

```bash
sudo ./ccp_kernel_load --ipc=0
```

Build the Rust clients used by the Python agent:

```bash
cd ICCP/iccp-rust/dtcc
cargo build

cd ../dtcc-portus
cargo build

cd ../orca
cargo build

cd ../orca-portus
cargo build

cd ../iccp
cargo build
```

Unload the kernel module when finished:

```bash
cd ICCP/iccp-rust
sudo ./ccp_kernel_unload
```

## Prepare Python Environment

Create and activate a Python environment, then install dependencies:

```bash
cd ICCP/iccp-python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For Conda-based setups, create an environment with Python 3.8 and install the
same runtime packages manually or from a curated `environment.yml`.

Install Cap'n Proto / pycapnp if they are not already available:

```bash
sudo apt install capnproto libcapnp-dev
pip install pycapnp
```

## Checkpoints

Place model checkpoints under `iccp-python/`:

```text
iccp-python/
├── dt_ckpt/
│   └── sfdandc_4token_best_checkpoint_iter_17.pkl
├── sage_ckpt/
│   ├── checkpoint
│   ├── ckpt-1.index
│   └── ckpt-1.data-00000-of-00001
└── orca_ckpt/
    └── tf2/
        ├── actor/
        ├── critic1/
        └── critic2/
```

The default DTCC checkpoint is selected by `--load_file`.

## Run Python Agent

Run DTCC with the lotus Rust client:

```bash
cd ICCP/iccp-python
python dtcc.py \
  --model_type dt \
  --device cpu \
  --arch lotus \
  --flows 1 \
  --bw 48
```

Run SAGE:

```bash
cd ICCP/iccp-python
python dtcc.py \
  --model_type sage \
  --device cpu \
  --arch lotus \
  --flows 1 \
  --bw 48
```

Run ORCA:

```bash
cd ICCP/iccp-python
python dtcc.py \
  --model_type orca \
  --device cpu \
  --arch lotus \
  --flows 1 \
  --bw 48
```

Use the Portus client instead of lotus:

```bash
python dtcc.py --model_type dt --device cpu --arch portus
```

Enable batched inference for multi-flow experiments:

```bash
python dtcc.py \
  --model_type dt \
  --device cpu \
  --arch lotus \
  --batch \
  --flows 8 \
  --rpc_ms 5
```

Run only the Python RPC server and start the Rust client manually:

```bash
python dtcc.py \
  --model_type dt \
  --device cpu \
  --standalone true \
  --rl_channel_addr 127.0.0.1:4826
```

## Hybrid ICCP Runtime

Hybrid mode uses `iccp-rust/iccp/target/debug/iccp` and currently supports the
lotus architecture:

```bash
cd ICCP/iccp-python
python dtcc.py \
  --model_type dt \
  --device cpu \
  --arch lotus \
  --hybrid_enable
```

The Rust hybrid runtime accepts:

```bash
../iccp-rust/iccp/target/debug/iccp \
  --default dtcc \
  --dtcc-addr 127.0.0.1:4826 \
  --dtcc-init-cwnd 10
```

Valid `--default` values are `dtcc`, `bbr`, and `orca`.

## Common Runtime Flags

| Flag | Meaning |
| --- | --- |
| `--model_type` | `dt`, `sage`, or `orca` |
| `--arch` | `lotus` or `portus` |
| `--flows` | Number of concurrent flows |
| `--bw` | Bandwidth hint used by the agent |
| `--batch` | Enable Python-side batched inference |
| `--batch_size` | Manual batch size; `0` means automatic sizing |
| `--rpc_ms` | Expected Python inference latency in ms |
| `--rpc_timeout` | Rust RPC timeout in ms; `0` uses automatic logic |
| `--resp_timeout` | Rust response timeout in ms; `0` uses automatic logic |
| `--bp_timeout` | BatchProcessor wait timeout in ms; `0` uses automatic logic |
| `--standalone` | Start the Python RPC server only |
| `--hybrid_enable` | Use the hybrid ICCP Rust runtime |

## Troubleshooting

- If Python cannot find a Rust binary, check the `../iccp-rust/` path in
  `decision_transformer/envs/cc.py` and confirm the Rust client has been built.
- If `pycapnp` fails to load schemas, run `dtcc.py` from `iccp-python/` so the
  relative `schema/` path resolves correctly.
- If CCP cannot be selected by `iperf3 -C ccp`, confirm the kernel module is
  loaded and the system allows custom TCP congestion-control modules.
- If TensorFlow or OpenMP starts too many threads, note that `dtcc.py` sets CPU
  thread environment variables before importing ML frameworks.

## License

This repository includes modified components from CCP-related projects. Check
the license files under `iccp-rust/`, `iccp-rust/libccp/`,
`iccp-rust/libccp-rust/`, and individual Rust subcrates before redistribution.

## Citation

If you use ICCP in academic work, please cite the corresponding ICCP / DTCC
paper or project documentation.
