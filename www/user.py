from orm import Model, StringField, IntegerField
import asyncio
class User(Model):
	"""docstring for User"""
	__table__ = 'user'

	id = IntegerField(primary_key=True)
	name = StringField()




