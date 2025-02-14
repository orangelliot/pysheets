from pysheets.src.pysheets import Schema, DataBase

a = Schema.load_or_create(name='test_schema', path='C:/Users/ellfisku/py_pkgs/test_schema.json', workspace_id=123456789)
#DataBase.create(a, name='test_db', folder_id=987654321)
a.save()

b = Schema.load('C:/Users/ellfisku/py_pkgs/test_schema.json').to_json()

print(b)