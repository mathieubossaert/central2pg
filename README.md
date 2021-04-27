# Odata2PG

Fonctions pl/pgsql de récupération des données d'ODK central vers une base de données PostgreSQL

PostgreSQL's functions to retreive datas from OData API to a PostgreSQL database 

Those functions make use of the "COPY FROM PROGRAM" PostgreSQL capability. The program called is curl. TSO curl need to be installed on your database server.
Security issues are for the moment bypassed with the use of -k function, considering we know the server called by curl.

## How to use it - Example

```sql
SELECT outils.get_submission_from_odata(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
	false,
	'__system%%2FsubmissionDate',
	'gt',
	'2020-10-01',
	'odk_central',
concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.')))))
)
FROM outils.get_form_tables_list_from_odata('my_login','my_password','odata_server.mydomaine.org',	5,'Sicen','__system%%2FsubmissionDate','gt','2020-10-01');

SELECT outils.feed_data_tables_from_odata('odk_central',concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.'))))))
FROM outils.get_form_tables_list_from_odata('my_login','my_password','odata_server.mydomaine.org',	5,'Sicen','__system%%2FsubmissionDate','gt','2020-10-01');
/*
SELECT * FROM odk_central.form_sicen_submissions_data;
SELECT * FROM odk_central.form_sicen_emplacements_data;
SELECT * FROM odk_central.form_sicen_observations_data;
*/
```

Functions are created in a schema named "outils". Adapt it to your needs.

## "Main" functions

### get_form_tables_list_from_central.sql
#### description
Returns the lists of "table" composing a form. The "core" one and each one corresponding to each repeat_group.
#### parameters : 
#### returning : 

### get_submission_from_central.sql
#### description
Get json data from Central, feed a temporary table with a generic name central_json_from_central.
Once the temp table is created and filled, PG checks if the destination (permanent) table exists. If not PG creates it with only one json column named "value".
PG does the same to check if a unique constraint on the \_\_id exists. This index will be use to ignore subissions already previously inserted in the table, using an "ON CONFLICT xxx DO NOTHING"
#### parameters : 
#### returning : 

### feed_data_tables_from_central.sql
#### description

### get_file_from_central_api.sql
#### description
Download each media mentioned in submissions
#### parameters : 
#### returning : 

## "Shadow" functions needed

### dynamic_pivot.sql
#### description
-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html (thanks again)
CREATE a pivot table dynamically, withut specifying mannually the row structure.
Returns a cursor use by both following finction to create a table and feed it
#### parameters : 
#### returning : 

### create_table_from_refcursor.sql
#### description
-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
Create a table corresponding to the curso structure (attribute types and names)
#### parameters : 
#### returning : 

### insert_into_from_refcursor.sql
#### description
-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
Feed the table with data
#### parameters : 
#### returning : 

