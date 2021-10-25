COPY enem (


)
FROM '/cassandra/csv/MICRODADOS_ENEM_2014_utf8.csv'
WITH header = true 
AND delimiter = ';'
AND CHUNKSIZE=1
AND NULL='null'
AND DECIMALSEP='.';