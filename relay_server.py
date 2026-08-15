# relay_server.py - WebSocket version for Render
import asyncio
import json
from datetime import datetime
import os
from aiohttp import web

connections = {}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    device_id = None
    device_name = None
    client_type = None
    
    try:
        # First message: registration
        msg = await ws.receive_json()
        device_id = msg.get('id')
        client_type = msg.get('type')
        device_name = msg.get('name', device_id)
        
        if not device_id or not client_type:
            await ws.close()
            return ws
        
        log(f"✓ Registered {client_type}: {device_name} ({device_id})")
        
        connections[device_id] = {
            'ws': ws,
            'type': client_type,
            'name': device_name
        }
        
        # Send acknowledgment
        await ws.send_json({'status': 'connected', 'device_id': device_id})
        log(f"Active devices: {list(connections.keys())}")
        
        # Message relay loop
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    # Handle PC list request
                    if data.get('type') == 'get_pc_list':
                        pc_list = []
                        for dev_id, dev_info in connections.items():
                            if dev_info['type'] == 'pc':
                                pc_list.append({
                                    'id': dev_id,
                                    'name': dev_info['name']
                                })
                        
                        await ws.send_json({'type': 'pc_list', 'pcs': pc_list})
                        log(f"Sent PC list to {device_id}: {len(pc_list)} PCs")
                        continue
                    
                    # Relay message to target
                    target = data.get('target')
                    if target and target in connections:
                        target_ws = connections[target]['ws']
                        await target_ws.send_str(msg.data)
                        log(f"→ {device_id} → {target}")
                    else:
                        await ws.send_json({
                            'status': 'error',
                            'message': f'Device {target} offline'
                        })
                        log(f"✗ Target {target} not found")
                
                except json.JSONDecodeError as e:
                    log(f"JSON error: {e}")
                except Exception as e:
                    log(f"Relay error: {e}")
            
            elif msg.type == web.WSMsgType.ERROR:
                log(f'WebSocket error: {ws.exception()}')
    
    except Exception as e:
        log(f"Handler error: {e}")
    finally:
        if device_id and device_id in connections:
            del connections[device_id]
            log(f"✗ Disconnected: {device_id}")
            log(f"Remaining: {list(connections.keys())}")
    
    return ws

async def health_check(request):
    active_pcs = sum(1 for v in connections.values() if v['type'] == 'pc')
    active_phones = sum(1 for v in connections.values() if v['type'] == 'phone')
    return web.Response(text=f"Relay Server OK\nPCs: {active_pcs} | Phones: {active_phones}\nTotal: {len(connections)}")

app = web.Application()
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/health', health_check)
app.router.add_get('/', health_check)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    log(f"Starting WebSocket relay on port {port}")
    web.run_app(app, host='0.0.0.0', port=port)
