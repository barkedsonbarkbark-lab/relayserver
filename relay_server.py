import asyncio
import json
import os

import websockets


HOST = os.getenv("RELAY_HOST", "127.0.0.1")
PORT = int(os.getenv("RELAY_PORT", "8765"))
TOKEN = os.getenv("RELAY_TOKEN", "change-me")

inputter_socket = None
controller_socket = None
latest_frame = None


async def send_json(websocket, payload):
    await websocket.send(json.dumps(payload))


async def handler(websocket):
    global inputter_socket, controller_socket, latest_frame

    try:
        msg = await websocket.recv()
        data = json.loads(msg)

        if data.get("token") != TOKEN:
            await send_json(websocket, {"type": "error", "data": "unauthorized"})
            await websocket.close()
            return

        role = data.get("role")

        if role == "inputter":
            inputter_socket = websocket
            print("[*] Inputter connected")

            async for message in websocket:
                try:
                    frame_data = json.loads(message)

                    if frame_data.get("type") == "frame":
                        latest_frame = frame_data.get("data")
                        if controller_socket:
                            await send_json(
                                controller_socket,
                                {"type": "frame", "data": latest_frame},
                            )
                    elif frame_data.get("type") == "response":
                        if controller_socket:
                            await send_json(
                                controller_socket,
                                {"type": "response", "data": frame_data.get("data")},
                            )
                except Exception as exc:
                    print(f"[!] Inputter message ignored: {exc}")

        elif role == "controller":
            controller_socket = websocket
            print("[*] Controller connected")

            if latest_frame:
                await send_json(websocket, {"type": "frame", "data": latest_frame})

            async for message in websocket:
                try:
                    cmd_data = json.loads(message)
                    if inputter_socket:
                        await send_json(inputter_socket, cmd_data)
                    else:
                        await send_json(
                            websocket,
                            {"type": "response", "data": "no_inputter_connected"},
                        )
                except Exception as exc:
                    print(f"[!] Controller message ignored: {exc}")
        else:
            await send_json(websocket, {"type": "error", "data": "unknown_role"})
            await websocket.close()

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as exc:
        print(f"[!] Connection error: {exc}")
    finally:
        if websocket == inputter_socket:
            inputter_socket = None
            print("[!] Inputter disconnected")
        if websocket == controller_socket:
            controller_socket = None
            print("[!] Controller disconnected")


async def main():
    async with websockets.serve(handler, HOST, PORT):
        print(f"[*] WebSocket relay running on {HOST}:{PORT}")
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
