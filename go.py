

# 35.203.92.156
################ Biblioteca
from time import time
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
import pandas as pd

cluster = Cluster(['35.203.92.156'])
session = cluster.connect()
formato = 'utf8' 

# Contador de tempo
def tic():
    global _start_time 
    _start_time = time()

def tac():
    t_sec = round(time.time() - _start_time)
    (t_min, t_sec) = divmod(t_sec,60)
    (t_hour,t_min) = divmod(t_min,60) 
    print('Duração: {}hour:{}min:{}sec'.format(t_hour,t_min,t_sec))

print ('# # Início # #')
#print(str(time() ))
################ Cria Keyspace
KEYSPACE = "bdm"
#KEYSPACE = "microdados"
session.set_keyspace(KEYSPACE)

#tic()
session = cluster.connect(KEYSPACE)
session.row_factory = tuple_factory
#rows = session.execute("SELECT IN_AMPLIADA_24, ST_CONCLUSAO FROM ENEM WHERE TP_LINGUA = 1;")
rows = session.execute("SELECT IN_AMPLIADA_24, ST_CONCLUSAO FROM ENEM WHERE nu_inscrical > 190000000000 AND NU_ANO = 2019 and tp_sexo = 1 limit 1;")
#rows = session.execute("alter table <table_name> with GC_GRACE_SECONDS = <timeout>;")
#rows = session.execute("alter table enem with GC_GRACE_SECONDS = 3600;")

#rows = session.execute("UPDATE COLUMN FAMILY cf with GC_GRACE = 86400;")

print (rows[0])

session.shutdown()
print ('# # FIM # #')
#print(str(time() ))

#tac()