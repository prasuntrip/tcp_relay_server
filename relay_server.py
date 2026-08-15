# relay_server.py
import asyncio
import json
from datetime import datetime

# Store active connections
# Format: {'pc_id': (reader, writer), 'phone_id': (reader, writer)}
connections = {}


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    log(f"New connection from {addr}")

    try:
        # First message identifies the client type
        data = await reader.readline()
        message = json.loads(data.decode().strip())

        client_type = message.get('type')  # 'pc' or 'phone'
        device_id = message.get('id')  # unique identifier

        if not client_type or not device_id:
            writer.close()
            await writer.wait_closed()
            return

        log(f"Registered {client_type}: {device_id}")

        # Store the connection
        connections[device_id] = (reader, writer)

        # Send acknowledgment
        response = json.dumps({'status': 'connected'}) + '\n'
        writer.write(response.encode())
        await writer.drain()

        # Relay messages
        while True:
            data = await reader.readline()
            if not data:
                break

            msg = json.loads(data.decode().strip())
            target = msg.get('target')  # ID of the device to send to

            if target in connections:
                target_reader, target_writer = connections[target]
                target_writer.write(data)
                await target_writer.drain()
                log(f"Relayed message from {device_id} to {target}")
            else:
                log(f"Target {target} not connected")

    except Exception as e:
        log(f"Error: {e}")
    finally:
        # Clean up on disconnect
        if device_id in connections:
            del connections[device_id]
            log(f"Disconnected: {device_id}")
        writer.close()
        await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, '0.0.0.0', 8888)
    log("Relay server started on port 8888")

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())