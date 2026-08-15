# relay_server.py
import asyncio
import json
from datetime import datetime
import os

# Store active connections
# Format: {'device_id': (reader, writer)}
connections = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    log(f"New connection from {addr}")
    
    device_id = None
    
    try:
        # First message identifies the client type
        data = await reader.readline()
        if not data:
            return
            
        message = json.loads(data.decode().strip())
        
        client_type = message.get('type')  # 'pc' or 'phone'
        device_id = message.get('id')      # unique identifier
        
        if not client_type or not device_id:
            log(f"Invalid registration from {addr}")
            writer.close()
            await writer.wait_closed()
            return
        
        log(f"✓ Registered {client_type}: {device_id}")
        
        # Store the connection
        connections[device_id] = (reader, writer)
        
        # Send acknowledgment
        response = json.dumps({'status': 'connected', 'device_id': device_id}) + '\n'
        writer.write(response.encode())
        await writer.drain()
        
        log(f"Active devices: {list(connections.keys())}")
        
        # Relay messages
        while True:
            data = await reader.readline()
            if not data:
                log(f"Connection closed by {device_id}")
                break
            
            try:
                msg = json.loads(data.decode().strip())
                target = msg.get('target')  # ID of the device to send to
                
                if target in connections:
                    target_reader, target_writer = connections[target]
                    target_writer.write(data)
                    await target_writer.drain()
                    log(f"→ Relayed from {device_id} to {target}")
                else:
                    log(f"✗ Target {target} not found. Available: {list(connections.keys())}")
                    # Send error back to sender
                    error_msg = json.dumps({
                        'status': 'error',
                        'message': f'Target device {target} not connected'
                    }) + '\n'
                    writer.write(error_msg.encode())
                    await writer.drain()
            except json.JSONDecodeError as e:
                log(f"JSON decode error: {e}")
            except Exception as e:
                log(f"Relay error: {e}")
    
    except Exception as e:
        log(f"Client handler error: {e}")
    finally:
        # Clean up on disconnect
        if device_id and device_id in connections:
            del connections[device_id]
            log(f"✗ Disconnected: {device_id}")
            log(f"Remaining devices: {list(connections.keys())}")
        
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass

async def main():
    port = int(os.environ.get('PORT', 8888))
    server = await asyncio.start_server(handle_client, '0.0.0.0', port)
    log(f"{'='*50}")
    log(f"  Relay Server Started")
    log(f"  Port: {port}")
    log(f"{'='*50}")
    
    async with server:
        await server.serve_forever()

if __name__ == '__main__':
    asyncio.run(main())
