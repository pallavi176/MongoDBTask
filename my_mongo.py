import pymongo
import csv
import pandas as pd
from custom_logger import CustomLogger

class MyMongo:
    log = CustomLogger.log("mongo.log")

    def __init__(self,db_name,coll_name):
        """
        This will initialize the connection
        """
        try:
            self.client = pymongo.MongoClient("mongodb+srv://test:test@cluster0.sg1qp.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
            self.db = self.create_database(db_name)
            self.coll = self.create_collection(coll_name)
            self.log.info("Connection created with mongo db atlas")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def show_databases(self):
        """
        This method will show all the databases present
        """
        try:
            self.log.info("Listing all the databases ")
            db_list = self.client.list_database_names()
            self.log.info(f"Databases available are: {db_list}")
            return db_list
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def create_database(self,db_name):
        """
        This method will create database
        """
        try:
            self.log.info(f"Creating database: {db_name}")
            db = self.client[db_name]
            return db
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def create_collection(self,coll_name):
        """
        This method will create collection
        """
        try:
            self.log.info(f"Creating collection: {coll_name} in database: {self.db}")
            collection = self.db[coll_name]
            return collection
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def insert_one_doc(self,doc):
        """
        This method will insert one document to collection
        """
        try:
            if isinstance(doc, dict):
                self.log.info(f"Inserting document:{doc} in collection: {self.coll}")
                self.coll.insert_one(doc)
                self.log.info("One record inserted.")
                return "One record inserted"
            else:
                self.log.error("Raising exception since dictionary is not passed")
                raise Exception(f"You have not entered a dictionary: {doc}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def insert_many_docs(self, docs):
        """
        This method will insert more than one documents to collection
        """
        try:
            if isinstance(docs, list):
                self.log.info(f"Inserting many documents in collection: {self.coll}")
                self.coll.insert_many(docs)
                self.log.info("Many records inserted.")
                return "Many records inserted"
            else:
                self.log.error("Raising exception since list is not passed")
                raise Exception(f"You have not entered a list: {docs}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def format_csv(self, input_file):
        """
        This method will correct the formatting in the carbon nanotube csv file
        """
        try:
            if input_file.endswith('.csv'):
                self.log.info(f"Reading file: {input_file}")
                data_list = []
                with open(input_file, "r") as f:
                    carbon_data = csv.reader(f, delimiter="\n")
                    for i in carbon_data:
                        data_list.append(i[0].split(';'))
                self.log.info("File read and stored to list")
                output_file = input_file.split('.')[0]+'_formatted.csv'
                with open(output_file, "w", newline="") as f:
                    wr = csv.writer(f)
                    wr.writerows(data_list)
                self.log.info(f"Formatted data written to file: {output_file}")
                return output_file
            else:
                self.log.error("Raising exception since csv file is not passed")
                raise Exception(f"You have not entered a csv file: {input_file}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def insert_from_csv(self, file_name):
        """
        This method will insert more than one documents to collection
        """
        try:
            if file_name.endswith('.csv'):
                self.log.info(f"Formatting input file: {file_name}")
                formatted_file=self.format_csv(file_name)
                df = pd.read_csv(formatted_file)
                data = df.to_dict(orient='records')
                self.log.info("Input File converted to list of dictionaries")
                msg=self.insert_many_docs(data)
                self.log.info(f"Data inserted from input file: {file_name} to collection: {self.coll}")
                return str(msg)+' from csv file'
            else:
                self.log.error("Raising exception since csv file is not passed")
                raise Exception(f"You have not entered a csv file: {file_name}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_one_doc(self,query=None):
        """
        This method will select & return first document from the collection
        """
        try:
            self.log.info(f"selecting first document from the collection: {self.coll}")
            res = ''
            if query:
                if isinstance(query, dict):
                    res = self.coll.find_one(query)
                else:
                    self.log.error("Raising exception since dictionary query is not passed in select_one_doc")
                    raise Exception(f"You have not entered a dictionary query: {query} in select_one_doc")
            else:
                res = self.coll.find_one()
            if res:
                return res
            else:
                return f"No document found with query: {query}"
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_all_docs(self,query=None):
        """
        This method will select & return all the documents from the collection
        """
        try:
            self.log.info(f"selecting all the documents from the collection: {self.coll}")
            data_list = []
            if query:
                if isinstance(query, dict):
                    data_list = [i for i in self.coll.find(query)]
                else:
                    self.log.error("Raising exception since dictionary query is not passed in select_all_docs")
                    raise Exception(f"You have not entered a dictionary query: {query} in select_all_docs")
            else:
                data_list = [i for i in self.coll.find()]
            if data_list:
                return data_list
            else:
                return f"No document found with query: {query}"
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_x_docs(self,x):
        """
        This method will select & return x documents from the collection
        """
        try:
            self.log.info(f"selecting x:{x} documents from the collection: {self.coll}")
            data_list = []
            for i in self.coll.find().limit(x):
                data_list.append(i)
            return data_list
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_docs_one_filter(self,**kwargs):
        """
        This method will select & return documents from the collection
        with matching 1 filter
        """
        try:
            if len(kwargs) == 1:
                self.log.info(f"selecting documents from the collection: {self.coll} with filter: {kwargs}")
                for k in kwargs:
                    c = k
                    if type(kwargs[k]) == list:
                        v = kwargs[k]
                    else:
                        v = [kwargs[k]]
                data_list = []
                for i in self.coll.find({c:{'$in':v}}):
                   data_list.append(i)
                return data_list
            else:
                self.log.error("Raising exception since 1 filter is not passed")
                raise Exception(f"You have not entered a filter: {kwargs} with size 1")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_docs_two_filters(self,**kwargs):
        """
        This method will select & return documents from the collection
        with matching 2 filters
        """
        try:
            if len(kwargs) == 2:
                self.log.info(f"selecting documents from the collection: {self.coll} with filter: {kwargs}")
                c=[]
                v=[]
                for k in kwargs:
                    c.append(k)
                    if type(kwargs[k]) == list:
                        v.append(kwargs[k])
                    else:
                        v.append([kwargs[k]])
                data_list = []
                for i in self.coll.find({c[0]:{'$in':v[0]},c[1]:{'$in':v[1]}}):
                   data_list.append(i)
                return data_list
            else:
                self.log.error("Raising exception since 2 filters is not passed")
                raise Exception(f"You have not entered filters: {kwargs} with size 2")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def select_docs_with_filters(self,**kwargs):
        """
        This method will select & return documents from the collection
        with matching filters
        """
        try:
            if len(kwargs) == 1:
                data_list = self.select_docs_one_filter(**kwargs)
                return data_list
            elif len(kwargs) == 2:
                data_list = self.select_docs_two_filters(**kwargs)
                return data_list
            else:
                self.log.error("Raising exception since more than 2 filters is passed")
                raise Exception(f"You have entered a filter: {kwargs} with size more than 2")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def update_one_doc_with_args(self,*args,**kwargs):
        """
        This method will find 1 document from the collection
        with matching 1 filter and update with new value
        """
        try:
            if len(args)==2 and len(kwargs) == 1:
                self.log.info(f"Updating 1 document from the collection: {self.coll} with filter: {args}")
                for k in kwargs:
                    c=k
                    if type(kwargs[k]) != list:
                        v=kwargs[k]
                    else:
                        self.log.error("Raising exception since list is passed in the value")
                        raise Exception(
                            f"You have not entered a filter:{args} with len 2 & values: {kwargs} with size 1")
                self.coll.find_one_and_update({args[0]:args[1]},{'$set':{c:v}})
                return f"Updated 1 document: {args[0]}:{args[1]} with values: {c}:{v}"
            else:
                self.log.error("Raising exception since value is not passed with size 1 and filter with size 2")
                raise Exception(f"You have not entered a filter:{args} with len 2 & values: {kwargs} with size 1")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def update_many_docs_with_args(self,*args,**kwargs):
        """
        This method will find many document from the collection
        with matching 1 filter and update with new value
        """
        try:
            print(args,kwargs)
            if len(args)==2 and len(kwargs) == 1:
                self.log.info(f"Updating many documents from the collection: {self.coll} with filter: {args}")
                for k in kwargs:
                    c=k
                    if type(kwargs[k]) != list:
                        v=kwargs[k]
                    else:
                        self.log.error("Raising exception since list is passed in the value")
                        raise Exception(
                            f"You have not entered a filter:{args} with len 2 & values: {kwargs} with size 1")
                self.coll.update_many({args[0]:args[1]},{'$set':{c:v}})
                return f"Updated many documents: {args[0]}:{args[1]} with values: {c}:{v}"
            else:
                self.log.error("Raising exception since value is not passed with size 1 and filter with size 2")
                raise Exception(f"You have not entered a filter:{args} with len 2 & values: {kwargs} with size 1")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def update_one_doc(self,old_data,new_data):
        """
        This method will find 1 document from the collection
        with matching 1 filter and update with new value
        """
        try:
            if isinstance(old_data,dict) and isinstance(new_data,dict):
                self.log.info(f"Updating 1 document from the collection: {self.coll} with filter: {old_data} by new value: {new_data}")
                self.coll.find_one_and_update(old_data,{'$set': new_data})
                return f"Updated 1 document with filter: {old_data} by new value: {new_data}"
            else:
                self.log.error("Raising exception since dictionary is not passed in update_one_doc")
                raise Exception(f"You have not entered dictionaries:{old_data} & {new_data} in update_one_doc")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def update_many_docs(self,old_data,new_data):
        """
        This method will find all documents from the collection
        with matching 1 filter and update with new value
        """
        try:
            if isinstance(old_data,dict) and isinstance(new_data,dict):
                self.log.info(f"Updating all documents from the collection: {self.coll} with filter: {old_data} by new value: {new_data}")
                self.coll.update_many(old_data,{'$set': new_data})
                return f"Updated all documents with filter: {old_data} by new value: {new_data}"
            else:
                self.log.error("Raising exception since dictionary is not passed in update_many_docs")
                raise Exception(f"You have not entered dictionaries:{old_data} & {new_data} in update_many_docs")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def delete_one_doc(self,query):
        """
        This method will find 1 document from the collection
        with matching 1 filter and delete it
        """
        try:
            if isinstance(query,dict):
                self.log.info(f"Deleting 1 document from the collection: {self.coll} with filter: {query}")
                self.coll.delete_one(query)
                return f"Deleted 1 document with filter: {query}"
            else:
                self.log.error("Raising exception since dictionary is not passed")
                raise Exception(f"You have not entered a dictionary:{query}")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def delete_many_docs(self,query):
        """
        This method will find all documents from the collection
        with matching 1 filter and delete them
        """
        try:
            if isinstance(query,dict):
                self.log.info(f"Deleting all documents from the collection: {self.coll} with filter: {query}")
                self.coll.delete_many(query)
                return f"Deleted all documents with filter: {query}"
            else:
                self.log.error("Raising exception since dictionary is not passed in delete_many_docs")
                raise Exception(f"You have not entered a dictionary:{query} in delete_many_docs")
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def drop_collection(self):
        """
        This method will drop the collection from the database
        """
        try:
            self.coll.drop()
            self.log.info(f"Collection: {self.coll} dropped.")
            return 'Collection dropped'
        except Exception as e:
            print(e)
            self.log.exception(str(e))

    def drop_database(self):
        """
        This method will drop the database
        """
        try:
            self.client.drop_database(self.db)
            self.log.info(f"Database: {self.db} dropped.")
            return 'Database dropped'
        except Exception as e:
            print(e)
            self.log.exception(str(e))