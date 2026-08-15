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
    
    try:
        # First message: registration
        msg = await ws.receive_json()
        device_id = msg.get('id')
        client_type = msg.get('type')
        
        if not device_id or not client_type:
            await ws.close()
            return ws
        
        log(f"✓ Registered {client_type}: {device_id}")
        connections[device_id] = ws
        
        await ws.send_json({'status': 'connected', 'device_id': device_id})
        log(f"Active: {list(connections.keys())}")
        
        # Message relay loop
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                target = data.get('target')
                
                if target in connections:
                    await connections[target].send_str(msg.data)
                    log(f"→ {device_id} → {target}")
                else:
                    await ws.send_json({'status': 'error', 'message': f'Device {target} offline'})
            elif msg.type == web.WSMsgType.ERROR:
                log(f'WebSocket error: {ws.exception()}')
    
    except Exception as e:
        log(f"Handler error: {e}")
    finally:
        if device_id and device_id in connections:
            del connections[device_id]
            log(f"✗ Disconnected: {device_id}")
    
    return ws

async def health_check(request):
    return web.Response(text=f"Relay Server OK\nActive devices: {len(connections)}")

app = web.Application()
app.router.add_get('/ws', websocket_handler)
app.router.add_get('/health', health_check)
app.router.add_get('/', health_check)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    log(f"Starting WebSocket relay on port {port}")
    web.run_app(app, host='0.0.0.0', port=port)
