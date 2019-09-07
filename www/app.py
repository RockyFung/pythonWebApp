import logging;logging.basicConfig(level=logging.INFO)
import asyncio, os, json, time
from datetime import datetime
from aiohttp import web

def index(request):
	return web.Response(body=b'<h1>index page</h1>',content_type='text/html')

async def init():
	app = web.Application()
	app.add_routes([web.get('/',index)])

	runner = web.AppRunner(app)
	await runner.setup()
	site = web.TCPSite(runner, 'localhost', 9000)
	await site.start()
	print('服务器启动成功！')

loop = asyncio.get_event_loop()
loop.run_until_complete(init())
loop.run_forever()
