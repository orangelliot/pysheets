import json
import os

TRASH_LIMIT = 15

####################################################
###################### SCHEMA ######################
####################################################

DEFAULT_SCHEMA = {
    'name': 'default_schema',
    'path': '',
    'workspace_id': 0,
    'trash': [],
    'dbs': {}
}

class Schema:

    def __init__(self, name, path, workspace_id, trash=DEFAULT_SCHEMA['trash'], dbs=DEFAULT_SCHEMA['dbs']):
        self.name = name
        self.path = path
        self.workspace_id = workspace_id
        self.trash = trash
        self.dbs = dbs

########### constructors ###########

    # constructor to create new schema
    @classmethod
    def create(cls, path, workspace_id, name):
        # ensure correct filetype
        if not path.endswith('.json'):
            path += '.json'

        # ensure no existing file
        if os.path.isfile(path):
            raise FileExistsError('There is already a schema at that location. If you want to load an existing schema, use Schema.load()')
        
        # define new schema
        schema = DEFAULT_SCHEMA.copy()
        schema['name'] = name
        schema['path'] = path
        schema['workspace_id'] = workspace_id

        # init new schema
        return cls(**schema)
    
    # constructor to load existing schema
    @classmethod
    def load(cls, path):
        # ensure file exists
        if not os.path.isfile(path):
            raise FileNotFoundError(f'no schema available to load at {path}')
        
        # attempt to load existing schema
        with open(path, mode='r') as f:
            schema_json = json.load(f)
        
        # init new schema
        schema = cls(**schema_json)
        
        # load dbs
        for db in schema.dbs:
            schema.dbs[db] = DataBase.load(schema, db)

        # return new schema
        return schema
    
    # constructor to load schema, or create one if none exists
    @classmethod
    def load_or_create(cls, name, path, workspace_id):
        # check if file exists
        if os.path.isfile(path):
            schema = Schema.load(path)

            # if there is an existing file with a different workspace_id or name raise an error
            if schema.name != name:
                print(f'Existing schema at {path} has a different name {schema.name} than provided {name}. Using existing name')
            if schema.workspace_id != workspace_id:
                raise ValueError(f'Existing schema at {path} has a different workspace_id {schema.workspace_id} than provided {workspace_id}')
            return schema
        else:
            # if file doesnt exist, create new schema
            return Schema.create(path, workspace_id, name)
        
########### end constructors ###########

    def save(self):
        with open(self.path, 'w+') as f:
            json.dump(self.to_json(), f)
    
    def to_json(self):
        self_dict = self.__dict__
        for db in self_dict['dbs']:
            self_dict['dbs'][db] = self_dict['dbs'][db].to_json()
        return self_dict

######################################################
###################### DATABASE ######################
######################################################

DEFAULT_DB = {
    'name': 'default_db',
    'schema': 'default_schema',
    'folder_id': 0,
    'tables': {}
}

class DataBase:
    tables = []
    def __init__(self, name, schema, folder_id, tables):
        self.name = name
        self.schema = schema
        self.folder_id = folder_id
        self.tables = tables

########### constructors ###########

    @classmethod
    def create(cls, schema, name, folder_id, tables=DEFAULT_DB['tables']):
        # ensure schema does not already have a db with same name or folder id
        if name in schema.dbs:
            raise ValueError(f'Existing db in {schema.name} with name {name}')
        if folder_id in sum(schema.dbs.values()):
            raise ValueError(f'Existing db in {schema.name} with folder_id {folder_id}')
        
        # create db
        db = cls(name, schema, folder_id, tables)
        schema.dbs[db.name] = db
        return db

    @classmethod
    def load(cls, schema, name):
        # create db from json
        db = cls(**schema.dbs[name])

        # raise value error if db is assigned to a different schema than it is being loaded under
        if db.schema != schema.name:
            raise ValueError(f'cannot load db {db.name} under schema {schema.name} as it is already assigned to {db.schema}')
        
        # assign schema as schema object
        db.schema = schema

        # load db object into schema
        schema.dbs[name] = db
        return db

    @classmethod
    def load_or_create(cls, folder_id, name, schema):
        pass
    
########### end constructors ###########

    def add_table(self):
        # add table to db
        pass

    def load_tables(self):
        # load all tables in db
        pass

    def save(self):
        pass

    def to_json(self):
        self_dict = self.__dict__
        self_dict['schema'] = self_dict['schema'].name
        for table in self_dict['tables']:
            self_dict['tables'][table] = self_dict['tables'][table].to_json()
        return self_dict

###################################################
###################### TABLE ######################
###################################################

DEFAULT_TABLE = {
    'name': 'default_table',
    'db': 'default_db',
    'sheet_id': 0,
    'ext_sheet_ids': [],
    'cols': {}
}

class Table:
    def __init__(self, name, db, sheet_id, ext_sheet_ids, cols):
        self.name = name
        self.db = db
        self.sheet_id = sheet_id
        self.ext_sheet_ids = ext_sheet_ids
        self.cols = cols
        pass

    @classmethod
    def create_from_df(cls, df, default_values, dtypes):
        # create table json from dataframe
        pass
    
    @classmethod
    def create(cls, name, db, sheet_id, ext_sheet_ids, cols):
        # ensure db doesnt have existing table with name or any sheet ids
        if name in db.tables:
            raise ValueError(f'Existing table in {db.name} with name {name}')
        if sheet_id in sum(db.tables.values()):
            raise ValueError(f'Existing table in {db.name} with sheet_id {sheet_id}')
        
        pass

    @classmethod
    def load(cls, name, db, sheet_id, ext_sheet_ids, cols):

        pass

    def load_data(self):
        # load data from smartsheet
        df = None
        self.data = df
        pass

    def save_data(self):
        # save data to smartsheet
        pass

    def to_json(self):
        return {'name': 'not_implemented'}