# RunPod A100 SXM Setup

This guide starts from a bare RunPod pod with:

- `1x A100 SXM (80 GB VRAM)`
- `117 GB RAM`
- `16 vCPU`
- a minimal Linux distro
- no Docker
- no common convenience tools installed yet

The goal is to run:

- `OpenGVLab/InternVL3_5-14B`
- `vLLM` on local port `8001`
- `Traefik` on public port `8000`
- `Supervisor` for process management

This guide is tuned for the current data-prep PDF/image workload:

- map-reduce style PDF processing
- OpenAI-compatible `/v1/*` API
- stronger throughput than the A40 setup

## Recommended target config

For this A100 80 GB setup, start with:

- `--gpu-memory-utilization 0.92`
- `--max-model-len 32768`
- `--max-num-seqs 2`
- `--max-num-batched-tokens 32768`
- Traefik `inFlightReq.amount: 2`

Pipeline-side recommended settings:

- `EUF_VISION_MIN_INTERVAL_SEC=0.5`
- `EUF_VISION_PDF_CHUNK_PAGES=4`
- `EUF_VISION_PDF_MAX_PAGES=50`
- `EUF_VISION_REDUCE_PARTS_PER_PASS=8`

## 1. Install base packages

On a bare Debian/Ubuntu-style RunPod instance:

```bash
apt-get update
apt-get install -y \
  curl \
  ca-certificates \
  tar \
  supervisor \
  python3 \
  python3-venv \
  nano \
  tmux \
  git \
  procps \
  iproute2
```

Optional but useful:

```bash
apt-get install -y htop jq
```

## 2. Install `uv` and `vllm`

Install `vllm` before starting Supervisor.

Official references:

- `uv` installer and overview:
  - https://docs.astral.sh/uv/
- `uv` pip-compatible interface:
  - https://docs.astral.sh/uv/pip/
- `uv` pip compatibility notes:
  - https://docs.astral.sh/uv/pip/compatibility/
- vLLM installation / quickstart:
  - https://docs.vllm.ai/en/v0.13.0/getting_started/quickstart/

Install `uv`:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source /root/.local/bin/env
uv --version
```

Create a virtual environment and install `vllm` with `uv`:

```bash
cd /workspace
uv venv .venv
source /workspace/.venv/bin/activate
uv pip install vllm
```

Verify:

```bash
source /workspace/.venv/bin/activate
which vllm
vllm --help | head
```

Notes:

- `uv` is generally a good choice here because it is faster and works with a pip-compatible interface.
- Official docs note that `uv pip` is compatible with common `pip` workflows, but it is not intended to be a perfect clone of `pip`.
- If `which vllm` prints nothing, Supervisor will fail with `BACKOFF` / `FATAL`, so do not continue until this is fixed.

## 3. Create the working directories

```bash
mkdir -p /workspace/bin
mkdir -p /workspace/logs
mkdir -p /workspace/ops
mkdir -p /workspace/traefik/dynamic
mkdir -p /workspace/vllm/vision/logs
mkdir -p /workspace/vllm/vision/.cache/huggingface/hub
mkdir -p /workspace/vllm/vision/.cache/huggingface/transformers
mkdir -p /workspace/vllm/vision/.cache/vllm
mkdir -p /workspace/vllm/vision/.cache/torch
mkdir -p /run/secrets
```

## 4. Install Traefik

```bash
cd /workspace/bin

TRAEFIK_TAG="$(curl -fsSL https://api.github.com/repos/traefik/traefik/releases/latest \
  | grep -m1 '"tag_name":' \
  | sed -E 's/.*"([^"]+)".*/\1/')"

echo "Latest Traefik tag: ${TRAEFIK_TAG}"

curl -fL \
  -o traefik_linux_amd64.tar.gz \
  "https://github.com/traefik/traefik/releases/download/${TRAEFIK_TAG}/traefik_${TRAEFIK_TAG}_linux_amd64.tar.gz"

tar --no-same-owner -xzf traefik_linux_amd64.tar.gz traefik
chmod +x traefik
./traefik version
```

If you prefer a more robust tag lookup, install `jq` and use:

```bash
apt-get install -y jq

TRAEFIK_TAG="$(curl -fsSL https://api.github.com/repos/traefik/traefik/releases/latest | jq -r .tag_name)"
echo "Latest Traefik tag: ${TRAEFIK_TAG}"
```

Note:

- `curl: (23) Failure writing output to destination` can happen when `grep -m1` exits early after finding the first match.
- That warning is not the real issue.
- The real failure happens if the `sed` expression is wrong and produces a literal `\1` instead of the actual tag.

## 5. Create the vLLM secret file

```bash
cat > /run/secrets/vllm_vision.env <<'EOF'
VLLM_API_KEY=your_token_here_without_spaces
EOF

chmod 600 /run/secrets/vllm_vision.env
```

## 6. Write the Traefik config

Create `/workspace/traefik/traefik.yml`:

```bash
nano /workspace/traefik/traefik.yml
```

Put this content in it:

```yaml
entryPoints:
  web:
    address: ":8000"

providers:
  file:
    directory: "/workspace/traefik/dynamic"
    watch: true

log:
  level: INFO

accessLog:
  filePath: "/workspace/logs/traefik_access.log"
```

Create `/workspace/traefik/dynamic/vllm_vlm.yml`:

```bash
nano /workspace/traefik/dynamic/vllm_vlm.yml
```

Put this content in it:

```yaml
http:
  routers:
    vlm-router:
      entryPoints:
        - web
      rule: "PathPrefix(`/v1`)"
      service: vlm-svc
      middlewares:
        - vlm-ratelimit
        - vlm-inflight

  services:
    vlm-svc:
      loadBalancer:
        servers:
          - url: "http://127.0.0.1:8001"

  middlewares:
    vlm-ratelimit:
      rateLimit:
        average: 4
        burst: 8

    vlm-inflight:
      inFlightReq:
        amount: 2
```

## 7. Write the Supervisor config

Create `/workspace/ops/supervisord.conf`:

```bash
nano /workspace/ops/supervisord.conf
```

Put this content in it:

```ini
[supervisord]
user=root
nodaemon=false
logfile=/workspace/logs/supervisord.log
pidfile=/workspace/ops/supervisord.pid

[supervisorctl]
serverurl=unix:///workspace/ops/supervisor.sock

[unix_http_server]
file=/workspace/ops/supervisor.sock
chmod=0700

[rpcinterface:supervisor]
supervisor.rpcinterface_factory = supervisor.rpcinterface:make_main_rpcinterface

[program:init_secrets]
command=/workspace/ops/init_secrets.sh
priority=1
autostart=true
autorestart=false
startsecs=0

[program:vllm_vision_8000]
directory=/workspace
priority=10

command=/bin/bash -lc 'set -a; source /run/secrets/vllm_vision.env; set +a; \
  source /workspace/.venv/bin/activate; \
  export CUDA_VISIBLE_DEVICES=0; \
  export HF_HOME=/workspace/vllm/vision/.cache/huggingface; \
  export HF_HUB_CACHE=/workspace/vllm/vision/.cache/huggingface/hub; \
  export TRANSFORMERS_CACHE=/workspace/vllm/vision/.cache/huggingface/transformers; \
  export VLLM_CACHE_DIR=/workspace/vllm/vision/.cache/vllm; \
  export TORCH_HOME=/workspace/vllm/vision/.cache/torch; \
  export PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True; \
  vllm serve OpenGVLab/InternVL3_5-14B \
    --host 127.0.0.1 \
    --port 8001 \
    --trust-remote-code \
    --dtype auto \
    --gpu-memory-utilization 0.92 \
    --max-model-len 32768 \
    --max-num-seqs 2 \
    --max-num-batched-tokens 32768 \
    --limit-mm-per-prompt "{\"image\":8}" \
    --served-model-name internvl3-5-14b \
    --api-key "$VLLM_API_KEY"'

autostart=true
autorestart=true
startsecs=20
stopsignal=TERM
stopasgroup=true
killasgroup=true

stdout_logfile=/workspace/vllm/vision/logs/vllm_8000.log
stderr_logfile=/workspace/vllm/vision/logs/vllm_8000.err.log

[program:traefik]
directory=/workspace
priority=20
command=/workspace/bin/traefik --configFile=/workspace/traefik/traefik.yml
autostart=true
autorestart=true
startsecs=2
stopsignal=TERM
stopasgroup=true
killasgroup=true
stdout_logfile=/workspace/logs/traefik_stdout.log
stderr_logfile=/workspace/logs/traefik_stderr.log
```

## 8. Create the init script

Create `/workspace/ops/init_secrets.sh`:

```bash
nano /workspace/ops/init_secrets.sh
```

Put this content in it:

```bash
#!/usr/bin/env bash
set -e

test -f /run/secrets/vllm_vision.env
chmod 600 /run/secrets/vllm_vision.env
```

Then:

```bash
chmod +x /workspace/ops/init_secrets.sh
```

## 9. Start Supervisor

First boot:

```bash
supervisord -c /workspace/ops/supervisord.conf
```

Then verify:

```bash
supervisorctl -c /workspace/ops/supervisord.conf status
```

You want:

- `vllm_vision_8000` -> `RUNNING`
- `traefik` -> `RUNNING`

## 10. Verify the service

Watch model startup:

```bash
supervisorctl -c /workspace/ops/supervisord.conf tail -f vllm_vision_8000
```

Check GPU:

```bash
nvidia-smi
```

Check listener:

```bash
ss -ltnp | grep 8000
```

Check the API:

```bash
curl -s http://127.0.0.1:8000/v1/models \
  -H "Authorization: Bearer $(grep '^VLLM_API_KEY=' /run/secrets/vllm_vision.env | cut -d= -f2-)"
```

## 11. Connect data-prep to this endpoint

In your data-prep `.env`:

```env
EUF_VISION_URL=http://<runpod-host>:8000
EUF_VISION_MODEL=internvl3-5-14b
EUF_VISION_API_KEY=your_token_here_without_spaces

EUF_VISION_MIN_INTERVAL_SEC=0.5
EUF_VISION_PDF_CHUNK_PAGES=4
EUF_VISION_PDF_MAX_PAGES=50
EUF_VISION_REDUCE_PARTS_PER_PASS=8
```

Then restart the data-prep app/container.

## 12. First validation run

Do not start with a full production-style batch first.

Start with:

1. one small PDF
2. one medium PDF
3. one 30-40 page PDF

Watch:

- data-prep `logs.txt`
- `supervisorctl ... tail -f vllm_vision_8000`
- `nvidia-smi`

## 13. Expected behavior

Compared to the A40 setup:

- lower per-request latency
- better map-reduce throughput
- more room for `32K` and `max-num-seqs=2`
- better fit for `chunk_pages=4`

But note:

- the pipeline is still serial at the app layer
- long PDFs still require many calls
- this is faster, not magically instantaneous

## 14. Safe tuning order

If this A100 setup is stable, tune in this order:

1. keep `32K`
2. keep `max-num-seqs=2`
3. test `EUF_VISION_PDF_CHUNK_PAGES=5`
4. only then consider:
   - higher Traefik in-flight limit
   - lower `EUF_VISION_MIN_INTERVAL_SEC`

Do not change all of these at once.

## 15. Backup and update procedure

Before changing config:

```bash
cp /workspace/ops/supervisord.conf /workspace/ops/supervisord.conf.bak
cp /workspace/traefik/traefik.yml /workspace/traefik/traefik.yml.bak
cp /workspace/traefik/dynamic/vllm_vlm.yml /workspace/traefik/dynamic/vllm_vlm.yml.bak
```

After edits:

```bash
supervisorctl -c /workspace/ops/supervisord.conf reread
supervisorctl -c /workspace/ops/supervisord.conf update
supervisorctl -c /workspace/ops/supervisord.conf restart vllm_vision_8000
supervisorctl -c /workspace/ops/supervisord.conf restart traefik
```

## 16. When to stop and roll back

Roll back if you see:

- repeated service restarts
- OOM-like model crashes
- repeated `429`
- worse quality than the A40 setup
- severe latency spikes under light load

If that happens, the first safe rollback is:

- `max-num-seqs=1`
- keep `32K`
- keep `chunk_pages=4`

That is the lowest-risk fallback on A100.

## 17. Troubleshooting

### Traefik tag lookup warning

If you see:

- `curl: (23) Failure writing output to destination`

after:

```bash
TRAEFIK_TAG="$(curl -fsSL ... | grep -m1 ... | sed ...)"
```

and:

```bash
echo "$TRAEFIK_TAG"
```

still shows a real version such as `v3.6.10`, then you can ignore that warning and continue.

Cause:

- `grep -m1` exits as soon as it finds the first match
- `curl` then briefly writes into a closed pipe

### Traefik extraction ownership warning

If you see:

- `tar: Cannot change ownership ... Operation not permitted`

then you used the plain extraction form by mistake.

Use:

```bash
tar --no-same-owner -xzf traefik_linux_amd64.tar.gz traefik
chmod +x /workspace/bin/traefik
/workspace/bin/traefik version
```

Do not keep retrying:

```bash
tar -xzf traefik_linux_amd64.tar.gz traefik
```

inside this RunPod/container environment.

### Quick verification

```bash
ls -l /workspace/bin/traefik
/workspace/bin/traefik version
```

### `vllm_vision_8000` exits immediately

Check whether `vllm` is installed inside the virtual environment:

```bash
source /workspace/.venv/bin/activate
which vllm
vllm --help | head
```

If `which vllm` prints nothing, install it with:

```bash
source /root/.local/bin/env
cd /workspace
uv venv .venv
source /workspace/.venv/bin/activate
uv pip install vllm
```
