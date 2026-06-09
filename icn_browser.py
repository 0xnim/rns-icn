#!/usr/bin/env python3
"""ICN Browser - Simple web UI to view ICN manifest and download files."""

import asyncio
import os
import json
from aiohttp import web
import aiohttp_jinja2
import jinja2

# Cached manifest (updated by background refresh)
MANIFEST_CACHE = {
    "entries": [
        {"kind": "blob", "label": "hello", "name": "/24cb54c7ec86294f0723e1d04015b8aa/hello", "size": "24"},
        {"kind": "blob", "label": "quote", "name": "/24cb54c7ec86294f0723e1d04015b8aa/quote", "size": "45"},
        {"kind": "blob", "label": "readme", "name": "/24cb54c7ec86294f0723e1d04015b8aa/readme", "size": "27"},
    ]
}
REFRESHING = False

async def refresh_manifest():
    """Run icn_client.py to fetch latest manifest."""
    global MANIFEST_CACHE, REFRESHING
    if REFRESHING:
        return
    REFRESHING = True
    print("[Browser] Refreshing manifest...")
    try:
        proc = await asyncio.create_subprocess_exec(
            "/Users/niklaswoj/rns-icn/.venv/bin/python3",
            "icn_client.py",
            env={**os.environ, "RNS_DEST": "24cb54c7ec86294f0723e1d04015b8aa"},
            cwd="/Users/niklaswoj/rns-icn",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90.0)
        output = stdout.decode()
        
        entries = []
        current = {}
        for line in output.split('\n'):
            line = line.strip()
            if line.startswith('[blob]'):
                if current:
                    entries.append(current)
                parts = line.split()
                if len(parts) >= 2:
                    current = {'kind': 'blob', 'label': parts[1]}
            elif line.startswith('Name:') and current:
                current['name'] = line.split('Name:')[1].strip()
            elif line.startswith('✓ Received') and current:
                current['size'] = line.split('(')[1].split(' ')[0] if '(' in line else '?'
                entries.append(current)
                current = {}
        
        if entries:
            MANIFEST_CACHE["entries"] = entries
            print(f"[Browser] Manifest refreshed: {len(entries)} entries")
    except Exception as e:
        print(f"[Browser] Refresh error: {e}")
    finally:
        REFRESHING = False

async def index(request):
    return aiohttp_jinja2.render_template('index.html', request, {
        'manifest': MANIFEST_CACHE,
        'refreshing': REFRESHING
    })

async def api_manifest(request):
    return web.json_response({
        "peer": "24cb54c7ec86294f0723e1d04015b8aa",
        "entries": MANIFEST_CACHE["entries"]
    })

async def trigger_refresh(request):
    asyncio.create_task(refresh_manifest())
    return web.json_response({"status": "refreshing"})

async def download_file(request):
    """Download a file by fetching it via icn_client."""
    label = request.match_info['label']
    
    try:
        # Run icn_client and capture raw output (same as refresh)
        proc = await asyncio.create_subprocess_exec(
            "/Users/niklaswoj/rns-icn/.venv/bin/python3",
            "icn_client.py",
            env={**os.environ, "RNS_DEST": "24cb54c7ec86294f0723e1d04015b8aa"},
            cwd="/Users/niklaswoj/rns-icn",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=90.0)
        
        # Parse output to find the requested file content
        output = stdout.decode()
        
        # Look for: "  [blob] label\n    Name: ...\n    ✓ Received (N bytes)\n       <content>"
        lines = output.split('\n')
        in_target = False
        content = None
        
        for i, line in enumerate(lines):
            if f'[blob] {label}' in line:
                in_target = True
            elif in_target and line.strip().startswith('✓ Received'):
                # Next line should be content
                if i + 1 < len(lines):
                    content = lines[i + 1].strip()
                    content = content.lstrip()
                break
            elif in_target and line.startswith('  [blob]') and label not in line:
                break
        
        if content:
            return web.Response(text=content, content_type='text/plain')
        else:
            return web.Response(text=f"File '{label}' not found in manifest", status=404)
           
    except Exception as e:
        return web.Response(text=f"Error fetching {label}: {e}", status=500)

def create_app():
    app = web.Application()
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('templates'))
    
    app.router.add_get('/', index)
    app.router.add_get('/api/manifest', api_manifest)
    app.router.add_post('/api/refresh', trigger_refresh)
    app.router.add_get('/download/{label}', download_file)
    
    return app

async def main():
    # Initial refresh
    await refresh_manifest()
    
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 8080)
    await site.start()
    print("\n[Browser] ICN Browser running at http://localhost:8080")
    print("Press Ctrl+C to stop\n")
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())