import orm, asyncio
from models import *

async def test_insert(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	u = User(name='test3', email='test3@126.com', passwd='123', image='about:blank')
	await u.save()

async def test_update(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	# 先查询
	u = await User.find('001')
	# 修改
	u.name = "lily"
	# u = User(id='001',name='lily', email='lily@126.com', passwd='11111', image='wwwwwwww')
	await u.update()

async def test_remove(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	# 先查询
	u = await User.find('001')
	if u:
		await u.remove()
	else:
		print('没有这条数据')
	

async def test_find(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	u = await User.find('001')
	print(u)

async def test_findAll(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	users = await User.findAll()
	for user in users:
		print(user)

async def test_findNumber(loop):
	await orm.create_pool(loop=loop, user='root', password='12345678', db='python')
	users = await User.findNumber('name')
	for user in users:
		print(user)





if __name__ == "__main__":
	loop = asyncio.get_event_loop()
	loop.run_until_complete(test_insert(loop))
	# loop.close()
	print('test finish')
