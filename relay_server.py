# relay_server.py - WebSocket version with better logging
import asyncio
import json
from datetime import datetime
import os
from aiohttp import web

connections = {}

def log(msg):
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {msg}", flush=True)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    device_id = None
    device_name = None
    client_type = None
    
    addr = request.remote
    log(f"New WebSocket connection from {addr}")
    
    try:
        # First message: registration
        msg = await ws.receive_json()
        log(f"Registration message: {msg}")
        
        device_id = msg.get('id')
        client_type = msg.get('type')
        device_name = msg.get('name', device_id)
        
        if not device_id or not client_type:
            log(f"❌ Invalid registration from {addr}: missing id or type")
            await ws.close()
            return ws
        
        log(f"✓ Registered {client_type}: {device_name} ({device_id})")
        
        connections[device_id] = {
            'ws': ws,
            'type': client_type,
            'name': device_name
        }
        
        # Send acknowledgment
        ack = {'status': 'connected', 'device_id': device_id}
        log(f">>> Sending ack to {device_id}: {ack}")
        await ws.send_json(ack)
        
        log(f"📊 Active devices ({len(connections)}): {list(connections.keys())}")
        
        # Message relay loop
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    log(f"<<< Message from {device_id}: {data}")
                    
                    # Handle PC list request
                    if data.get('type') == 'get_pc_list':
                        pc_list = []
                        for dev_id, dev_info in connections.items():
                            if dev_info['type'] == 'pc':
                                pc_list.append({
                                    'id': dev_id,
                                    'name': dev_info['name']
                                })
                        
                        response = {'type': 'pc_list', 'pcs': pc_list}
                        log(f">>> Sending PC list to {device_id}: {response}")
                        await ws.send_json(response)
                        log(f"✓ PC list sent successfully ({len(pc_list)} PCs)")
                        continue
                    
                    # Relay message to target
                    target = data.get('target')
                    if target:
                        if target in connections:
                            target_ws = connections[target]['ws']
                            log(f"📨 Relaying from {device_id} to {target}")
                            await target_ws.send_str(msg.data)
                            log(f"✓ Message relayed successfully")
                        else:
                            error_msg = {'status': 'error', 'message': f'Device {target} offline'}
                            log(f"❌ Target {target} not found. Sending error to {device_id}")
                            await ws.send_json(error_msg)
                    else:
                        log(f"⚠️ Message from {device_id} has no target")
                
                except json.JSONDecodeError as e:
                    log(f"❌ JSON decode error: {e}")
                except Exception as e:
                    log(f"❌ Relay error: {e}")
            
            elif msg.type == web.WSMsgType.ERROR:
                log(f'❌ WebSocket error from {device_id}: {ws.exception()}')
    
    except Exception as e:
        log(f"❌ Handler error for {device_id}: {e}")
    finally:
        if device_id and device_id in connections:
            del connections[device_id]
            log(f"✗ Disconnected: {device_id}")
            log(f"📊 Remaining devices ({len(connections)}): {list(connections.keys())}")
        
        try:
            await ws.close()
        except:
            pass
    
    return ws

async def health_check(request):
    active_pcs = sum(1 for v in connections.values() if v['type'] == 'pc')
    active_phones = sum(1 for v in connections.values() if v['type'] == 'phone')
    
    device_list = "\n".join([f"  - {k} ({v['type']}): {v['name']}" for k, v in connections.items()])
    
    return web.Response(text=f"""Relay Server OK
PCs: {active_pcs} | Phones: {active_phones}
Total: {len(connections)}

Connected Devices:
{device_list if device_list else '  (none)'}
""")

app = web.Application()
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/health', health_check)
app.router.add_get('/', health_check)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    log(f"🚀 Starting WebSocket relay server on port {port}")
    web.run_app(app, host='0.0.0.0', port=port)
