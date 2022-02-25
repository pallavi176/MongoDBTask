from my_mongo import MyMongo
from bson.objectid import ObjectId

DB = 'carbon'
COLLECTION = 'carbon_nanotubes'
FILE_NAME='carbon_nanotubes.csv'

conn = MyMongo(DB,COLLECTION)

print(conn.db)
print(conn.coll)

#Show existing database
print(conn.show_databases())

# Selecting all documents from collection
print(conn.select_all_docs())

# Bulk insert from csv file
print(conn.insert_from_csv(FILE_NAME))

# Selecting all documents inserted
print(conn.select_all_docs())

# Selecting all documents count from collection
print(len(conn.select_all_docs()))

#Show existing database
print(conn.show_databases())

# Inserting 1 document
doc1={'Chiral indice n': 5,
  'Chiral indice m': 4,
  'Initial atomic coordinate u': '0,704904',
  'Initial atomic coordinate v': '0,872491',
  'Initial atomic coordinate w': '0,630191',
  "Calculated atomic coordinates u'": '0,709336',
  "Calculated atomic coordinates v'": '0,880125',
  "Calculated atomic coordinates w'": '0,630465'}

print(conn.insert_one_doc(doc1))

# Selecting all documents count from collection
print(len(conn.select_all_docs()))

#Inserting many records
doc2=[
{'Chiral indice n': 5,
  'Chiral indice m': 4,
  'Initial atomic coordinate u': '0,129304',
  'Initial atomic coordinate v': '0,277928',
  'Initial atomic coordinate w': '0,050956',
  "Calculated atomic coordinates u'": '0,122792',
  "Calculated atomic coordinates v'": '0,274086',
  "Calculated atomic coordinates w'": '0,050998'},
 {'Chiral indice n': 5,
  'Chiral indice m': 4,
  'Initial atomic coordinate u': '0,145816',
  'Initial atomic coordinate v': '0,221293',
  'Initial atomic coordinate w': '0,089208',
  "Calculated atomic coordinates u'": '0,138865',
  "Calculated atomic coordinates v'": '0,215329',
  "Calculated atomic coordinates w'": '0,089298'}
]

print(conn.insert_many_docs(doc2))

# Selecting all documents count from collection
print(len(conn.select_all_docs()))

# Selecting 5 documents from collection
print(conn.select_x_docs(5))
print(len(conn.select_x_docs(5)))

# Selecting 1 document
print(conn.select_one_doc())

# Selecting 1 document with filter
print(conn.select_one_doc({"Calculated atomic coordinates w'":{'$in': ['0,157373','0,088712']}}))

# Selecting many documents with filter
print(conn.select_all_docs({"Calculated atomic coordinates w'":{'$in': ['0,157373','0,088712']}}))

# Selecting all documents
print(conn.select_all_docs())

# Selecting many documents with filter before update
query={"Calculated atomic coordinates w'":{'$in': ['0,157373','0,088712']}}
print(conn.select_all_docs(query))

#Updating 1 document
old_values={"Calculated atomic coordinates w'":'0,157373'}
set_values ={'Chiral indice n':50}
print(conn.update_one_doc(old_values,set_values))

# Selecting many documents with filter after update
print(conn.select_all_docs(query))

# Selecting many documents with filter before many updates
query={'Chiral indice n':3,'Chiral indice m':2}
print(conn.select_all_docs(query))

#Updating many documents
set_values ={'Initial atomic coordinate u':'1000,0'}
print(conn.update_many_docs(query,set_values))

# Selecting many documents with filter after update
print(conn.select_all_docs(query))

# Selecting 1 document before delete
#query= {'_id': ObjectId('62186a427bdfed1ac10de28b')}
query= {'Initial atomic coordinate v': '0,690165'}
print(conn.select_one_doc(query))

# Deleting 1 record
print(conn.delete_one_doc(query))

# Selecting 1 document after delete
print(conn.select_one_doc(query))

# Selecting documents before delete
query={'Chiral indice n':3,'Chiral indice m':2}
print(conn.select_all_docs(query))

# Deleting many documents
print(conn.delete_many_docs(query))

# Selecting documents after delete
print(conn.select_all_docs(query))

# Drop Collection
print(conn.drop_collection())

# Drop Database
#print(conn.drop_database())


