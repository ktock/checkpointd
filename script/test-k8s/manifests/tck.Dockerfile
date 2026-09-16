# Copyright 2026 checkpointd authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the A2A Technology Compatibility Kit into a throwaway image, run as an in-cluster pod by test.sh's tck subtest.
#   docker build -f script/test-k8s/manifests/tck.Dockerfile -t <ref> script/test-k8s/manifests
FROM python:3.12-slim

# Pinned commit that tck_agent was designed against; keep in sync if tck_agent tracks a newer one.
ARG TCK_COMMIT=5996b79f9cefa6fc390980e383e358a66fb9e49e

RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

WORKDIR /tck
RUN git init -q . \
    && git remote add origin https://github.com/a2aproject/a2a-tck.git \
    && git fetch -q --depth 1 origin "${TCK_COMMIT}" \
    && git checkout -q FETCH_HEAD

# The TCK's own hardcoded httpx timeout is too tight for this test's real actor latency, so it's patched at build time.
RUN sed -i 's/httpx\.Timeout(5\.0, read=30\.0)/httpx.Timeout(30.0, read=90.0)/' \
    tck/transport/jsonrpc_client.py tck/transport/http_json_client.py \
    && grep -q 'Timeout(30.0, read=90.0)' tck/transport/jsonrpc_client.py \
    && grep -q 'Timeout(30.0, read=90.0)' tck/transport/http_json_client.py

# Some test bodies bypass the patched clients and use httpx's own global default, so that default is patched too.
RUN pip install --no-cache-dir . \
    && python3 -c "\
import pathlib, httpx._config as c; \
p = pathlib.Path(c.__file__); \
s = p.read_text(); \
old = 'DEFAULT_TIMEOUT_CONFIG = Timeout(timeout=5.0)'; \
assert old in s, 'httpx._config.py no longer matches the expected default -- update this patch'; \
p.write_text(s.replace(old, 'DEFAULT_TIMEOUT_CONFIG = Timeout(timeout=90.0)'))" \
    && python3 -c "import httpx._config as c; assert c.DEFAULT_TIMEOUT_CONFIG.connect == 90.0, c.DEFAULT_TIMEOUT_CONFIG"

ENTRYPOINT ["python3", "run_tck.py"]
