import logging;logging.basicConfig(level=logging.INFO)
import asyncio
import aiomysql

# 打印sql
def log(sql, args=()):
    logging.info('SQL: %s' % sql)

# 创建连接池
async def create_pool(loop, **kw):
	logging.info('建立数据库连接池。。。')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port', 3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit',True),
		maxsize=kw.get('maxsize',10),
		minsize=kw.get('minsize',1),
		loop=loop
	)

# 查询
async def select(sql,args,size=None):
	log(sql,args)
	# global __pool
	async with  __pool.get() as conn:
		async with conn.cursor(aiomysql.DictCursor) as cur:
			await cur.execute(sql.replace('?','%s'), args or ())
			if size:
				rs = await cur.fetchmany(size)
			else:
				rs = await cur.fetchall()
		logging.info('返回%s条数据' % len(rs))
		return rs

# 增删改
async def execute(sql,args,autocommit=True):
	log(sql)
	print(args)
	# global __pool
	async with  __pool.get() as conn:
		if not autocommit:
			await conn.beggin()
		try:
			async with conn.cursor(aiomysql.DictCursor) as cur:
				await cur.execute(sql.replace('?','%s'),args)
				affected = cur.rowcount
			if not autocommit:
				await cur.close()	
		except BaseException as e:
			if not autocommit:
				await conn.rollback()
			raise
		return affected



# sql语句段占位符  
def create_args_string(num):
    L = []
    for n in range(num):
        L.append('?')
    return ', '.join(L)
# 定义Field父类
class Field(object):
	"""docstring for Field"""
	def __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' % (self.__class__.__name__, self.column_type, self.name)

# 映射各个field		
class StringField(Field):
	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name,ddl,primary_key,default)

class BooleanField(Field):
    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default)

class IntegerField(Field):
    def __init__(self, name=None, primary_key=False, default=0):
        super().__init__(name, 'bigint', primary_key, default)

class FloatField(Field):
    def __init__(self, name=None, primary_key=False, default=0.0):
        super().__init__(name, 'real', primary_key, default)

class TextField(Field):
    def __init__(self, name=None, default=None):
        super().__init__(name, 'text', False, default)

# model元类
# 任何继承自Model的类（比如User），会自动通过ModelMetaclass扫描映射关系，并存储到自身的类属性如__table__、__mappings__中。
class ModelMetaclass(type):
	# __new__方法接受的参数依次是：
    # 1.当前准备创建的类的对象（cls）
    # 2.类的名字（name）
    # 3.类继承的父类集合(bases)
    # 4.类的方法集合(attrs)
	def __new__(cls,name,bases,attrs):
		# 排除model本身
		if name=='Model':
			return type.__new__(cls,name,bases,attrs)
		# 获取table名称,没有定义__table__的值，就用类本身的名称name
		tableName = attrs.get('__table__',None) or name
		logging.info('found model:%s(table:%s)' % (name, tableName))

		# 获取所有的field和主键名
		mappings = dict()
		fields = []
		primaryKey = None
		for k,v in attrs.items():
			if isinstance(v,Field):
				logging.info('found mapping:%s ==> %s' % (k, v))
				mappings[k] = v
				# 如果有主键就设置为主键，不是主键则添加到字段
				if v.primary_key:
					# 找到主键
					if primaryKey:
						raise RuntimeError('<主键重复>Duplicate primary key for field:%s' % k)
					primaryKey = k
				else:
					fields.append(k)

		if not primaryKey:
			raise StandardError('Primary key not found')

		# 结合之前，即把之前在方法集合中的零散的映射删除，
        # 把它们从方法集合中挑出，组成一个大方法__mappings__
        # 把__mappings__添加到方法集合attrs中
		for k in mappings.keys():
			attrs.pop(k)
		# map(function, iterable, ...)
		# 把fields中的每个item转换成字符
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		# 保存属性和列的映射关系
		attrs['__mappings__'] = mappings
		# 表名
		attrs['__table__'] = tableName
		# 主键
		attrs['__primary_key__'] = primaryKey
		# 其他字段
		attrs['__fields__'] = fields

		# 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ','.join(escaped_fields),primaryKey, create_args_string(len(escaped_fields) + 1) )
		# .ambda  把每个mappings的item转变成 x=?  格式
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ','.join(map(lambda f:'`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls,name,bases,attrs)

# 定义model
# 编写Model基类继承自dict中，这样可以使用一些dict的方法
# ModelMetaclass 自定义元类
class Model(dict, metaclass=ModelMetaclass):
	 # 调用父类，即dict的初始化方法
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	# 让获取key的值不仅仅可以d[k]，也可以d.k
	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r'<Model> object has no attribute %s' %key)

	# 允许动态设置key的值，不仅仅可以d[k]，也可以d.k
	def __setattr__(self, key, value):
		self[key]=value

	def getValue(self, key):
		return getattr(self,key,None)

	def getValueOrDefault(self,key):
		value = getattr(self,key,None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s:%s' % (key, str(value)))
				setattr(self, key, value)
		return value

	@classmethod
	async def findAll(cls, where=None, args=None, **kw):
		sql = [cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args =[]
		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) == 2:
				sql.append('?, ?')
				args.extend(limit)
			else:
				raise ValueError('Invalid limit value: %s' % str(limit))
		# 返回的rs是一个元素是tuple的list
		rs = await select(' '.join(sql), args)
		#  **r 是关键字参数，构成了一个cls类的列表，其实就是每一条记录对应的类实例
		return [cls(**r) for r in rs]

	@classmethod
	async def findNumber(cls, selectField, where=None, args=None):
		sql = ['select %s _num_ from `%s`' % (selectField, cls.__table__)]
		if where:
			sql.append('where')
			sql.append(where)
		rs = await select(' '.join(sql), args, 1)
		if len(rs) == 0:
			return None
		return map(lambda n: n['_num_'], rs)
		# return rs[0]['_num_']

	@classmethod
	async def find(cls, pk):
		rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])

	# 以下为实例方法
	async def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__insert__, args)
		if rows !=1:
			logging.warn('failed to insert record:affected rows:%s' % rows)
		else:
			logging.info('保存成功: %s' % args)

	async def update(self):
		args = list(map(self.getValue, self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = await execute(self.__update__, args)
		if rows != 1:
			logging.warn('failed to update by primary key: affected rows:%s' % rows)
		else:
			logging.info('更新成功: %s' % args)

	async def remove(self):
		args = [self.getValue(self.__primary_key__)]
		rows = await execute(self.__delete__, args)
		if rows != 1:
			logging.warn('failed to remove by primary key: affected rows: %s' % rows)
		else:
			logging.info('删除成功: %s' % args)




		
		























