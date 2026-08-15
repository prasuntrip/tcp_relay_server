# relay_server.py
import asyncio
import json
from datetime import datetime
import os

# Store active connections with metadata
# Format: {'device_id': {'socket': (reader, writer), 'type': 'pc'/'phone', 'name': 'display_name'}}
connections = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def handle_client(reader, writer):
    addr = writer.get_extra_info('peername')
    log(f"New connection from {addr}")
    
    device_id = None
    
    try:
        # First message identifies the client
        data = await reader.readline()
        if not data:
            return
            
        message = json.loads(data.decode().strip())
        
        client_type = message.get('type')  # 'pc' or 'phone'
        device_id = message.get('id')
        device_name = message.get('name', device_id)  # Friendly name (for PCs)
        
        if not client_type or not device_id:
            log(f"Invalid registration from {addr}")
            writer.close()
            await writer.wait_closed()
            return
        
        log(f"✓ Registered {client_type}: {device_name} ({device_id})")
        
        # Store connection with metadata
        connections[device_id] = {
            'socket': (reader, writer),
            'type': client_type,
            'name': device_name
        }
        
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
                
                # Special command: Get list of available PCs
                if msg.get('type') == 'get_pc_list':
                    pc_list = []
                    for dev_id, dev_info in connections.items():
                        if dev_info['type'] == 'pc':
                            pc_list.append({
                                'id': dev_id,
                                'name': dev_info['name']
                            })
                    
                    response = json.dumps({
                        'type': 'pc_list',
                        'pcs': pc_list
                    }) + '\n'
                    writer.write(response.encode())
                    await writer.drain()
                    log(f"Sent PC list to {device_id}: {len(pc_list)} PCs")
                    continue
                
                # Normal message relay
                target = msg.get('target')
                
                if target in connections:
                    target_reader, target_writer = connections[target]['socket']
                    target_writer.write(data)
                    await target_writer.drain()
                    log(f"→ {device_id} → {target}")
                else:
                    log(f"✗ Target {target} not found")
                    error_msg = json.dumps({
                        'status': 'error',
                        'message': f'Device {target} offline'
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
        if device_id and device_id in connections:
            del connections[device_id]
            log(f"✗ Disconnected: {device_id}")
            log(f"Remaining: {list(connections.keys())}")
        
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
