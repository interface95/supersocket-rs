#!/usr/bin/env python3
"""Checks WebSocket per-message compression against the `websockets` library (pip install
websockets), which negotiates permessage-deflate by default, as browsers do.

As a client of the echo server example:

    cargo run --example websocket_echo_server --features websocket,deflate
    python3 scripts/websocket_deflate_interop.py client ws://127.0.0.1:4040/

As an echo server for the client example:

    python3 scripts/websocket_deflate_interop.py serve 8765
    cargo run --example websocket_client --features websocket,client,deflate -- ws://127.0.0.1:8765/
"""

import asyncio
import os
import sys

import websockets

MESSAGES = [
    "hello",
    "x" * 100_000,
    os.urandom(50_000),
    "",
    b"",
    "héllo wörld " * 1000,
    bytes(range(256)) * 400,
]


async def client(url: str) -> None:
    async with websockets.connect(url, compression="deflate", max_size=4 * 1024 * 1024) as ws:
        print("negotiated:", ws.response.headers.get("Sec-WebSocket-Extensions"), flush=True)
        for message in MESSAGES:
            await ws.send(message)
            reply = await ws.recv()
            assert reply == message, (type(message).__name__, len(message))
            print(f"ok: {type(message).__name__} of {len(message)} bytes echoed")
        await ws.close()
    print("all messages echoed")


async def serve(port: int) -> None:
    async def echo(ws):
        print("client offered:", ws.request.headers.get("Sec-WebSocket-Extensions"), flush=True)
        print("negotiated:", ws.response.headers.get("Sec-WebSocket-Extensions"), flush=True)
        async for message in ws:
            await ws.send(message)

    async with websockets.serve(
        echo, "127.0.0.1", port, compression="deflate", max_size=4 * 1024 * 1024
    ):
        print(f"echo server with permessage-deflate on ws://127.0.0.1:{port}/")
        await asyncio.Future()


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "client":
        asyncio.run(client(sys.argv[2]))
    elif len(sys.argv) == 3 and sys.argv[1] == "serve":
        asyncio.run(serve(int(sys.argv[2])))
    else:
        print(__doc__)
        sys.exit(2)
