from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,MetaData, Table, insert, Column,Integer, String,update,delete
#import urllib

#conn = urllib.quote_plus("Driver={ODBC Driver 13 for SQL Server};Server=tcp:idowu.database.windows.net,1433;Database=idowu;Uid=idowu@idowu;Pwd=@delek34;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;")
class DatabaseMarshal(object):

    #initialize class with database name and table name
    def __init__(self, db_name="sqlite:///data.db", table_name="my_db_table2"):
        self.metadata = MetaData()
        self.engine = create_engine(db_name)      #create an engine that is connected to the database
        self.session = sessionmaker(bind=self.engine)()
        self.table = None
        self.table_name = table_name
        self.table_list = []

    #create table with column details
    def create_table(self,table_details):
        try:
            self.table = Table(self.table_name, self.metadata, Column('id', Integer, primary_key=True), )
            self.table.metadata.create_all(self.engine)
            self._create_columns(table_details)
            return ('table created',201)
        except:
            return ('an error occured while creating table')
        

    #method used by create colums to add columns to created table
    def _add_column(self, column):
            column_name = column.compile(dialect=self.engine.dialect)
            column_type = column.type.compile(self.engine.dialect)
            self.engine.execute('ALTER TABLE %s ADD COLUMN %s %s' % (self.table_name, column_name, column_type))


    #method used by create table to convert data type to sqlalchemy type
    def _initiate_column(self, col_name, col_type):
        if col_type is "int":
            col_type = Integer
        elif col_type is "str":
            col_type = String
        else:
            col_type = String
        col = Column(col_name, col_type)
        return (col)


    #add specified columns to created tables
    def _create_columns(self, table_details):
        for item in table_details:
            col_name = item.get('column')
            col_type = item.get('type')
            self.table_list.append(self._initiate_column(col_name, col_type))

        for item in self.table_list:
            self._add_column(item)


    #insert record into column
    def insert_record(self, record_details):
        try:
            self.table = Table(self.table_name, self.metadata,autoload_with=self.engine)

            i = insert(self.table)
            i = i.values(record_details)
            self.session.execute(i)
            self.session.commit()
            return('record inserted',200)
        except:
            self.session.rollback()   #rollsback a session if error exist
            return ('error',400)
    

    #update rcord in table
    def update_record(self,_id,record_details):
        try:
            self.table = Table(self.table_name, self.metadata,autoload_with=self.engine)
            i = update(self.table).where(self.table.c.id==_id)
            i = i.values(record_details)
            self.session.execute(i)
            self.session.commit()
            return ('record updated', 200)
        except:
            self.session.rollback() #rollsback a session if error exist
            return ('error',400)
    


    #delete record in table
    def delete_record(self,_id):
        try:
            self.table = Table(self.table_name, self.metadata,autoload_with=self.engine)
            i = delete(self.table).where(self.table.c.id==_id)
            self.session.execute(i)
            self.session.commit()
            return ('record deleted', 200)

        except:
            self.session.rollback()  #rollsback a session if error exist
            return ('error',400)


test=DatabaseMarshal()
#test.create_table([{'column':'book2','type':'str'}])
test.insert_record({"book": "iidowu", "book2": "pass"})
test.update_record(1,{"book": "iidowuade", "book2": "passwp"})
test.delete_record(2)
