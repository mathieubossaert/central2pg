# Central2PG
## PostgreSQL's functions to retrieve datas from ODK Central's OData API to a PostgreSQL database 

Fonctions pl/pgsql de récupération des données d'ODK central vers une base de données PostgreSQL

Those functions make use of the "COPY FROM PROGRAM" PostgreSQL capability. The called program is curl. So curl need to be installed on your database server.
Security issues are for the moment bypassed with the use of -k function, considering we know the server called by curl.

## How to use it - Example

Simply run [central2pg.sql]](https://github.com/mathieubossaert/central2pg/blob/master/central2pg.sql) script in your database after you checked curl is installed.
Functions are created in a schema named "odk_central". Adapt it to your needs.
And start retrieving data from Central.

```sql
SELECT odk_central.odk_central_to_pg(
	'me@mydomain.org',			-- user
	'PassW0rd',				-- password
	'my_central_server.org',		-- central FQDN
	2, 					-- the project id, 
	'my_form_about_birds',			-- form ID
	'odk_data',				-- schema where to creta tables and store data
	'point_auto,point,ligne,polygone'	-- columns to ignore in json transformation to database attributes (geojson fields of GeoWidgets)
);

-- It now replaces
/*
SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
	'odk_central',
	concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.')))))
)
FROM odk_central.get_form_tables_list_from_central('me@mydomain.org','PassW0rd','my_central_server.org',	2,'my_form_about_birds');

SELECT odk_central.feed_data_tables_from_central('odk_central',concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.'))))))
FROM odk_central.get_form_tables_list_from_central('me@mydomain.org','PassW0rd','my_central_server.org',	2,'my_form_about_birds');
*/

```

This will automatically : 
* ask Central (at my_central_server.org) for the table list of the form "my_form_about_birds"
* get data for each table
* create those tables (one text attribute per form question) to stores those data in the schema "odk_data" of my database
* the last parameter lists the question to ignore in json exploration recusrion (geowidgets columns)
* feed those tables with the retrieved data

And at next call : 
* check for new form questions / table attributes
* create it if needed
* insert new data

## Complete process
```sql

SELECT odk_central.odk_central_to_pg('me@mydomain.org', 'PassW0rd', 'my_central_server.org', 2, 'my_form_about_birds', 'odk_data');

/* 
	This is a view build upon generated data tables for our particular needs. It shows only new data (wich are not already in our internal database)
*/

REFRESH MATERIALIZED VIEW odk_central.my_form_about_birds_new_data;

/* 	
	Here we get attachments for new data with attachment.
*/

SELECT outils.get_file_from_central(
	submission_id,
	prise_image,
	'me@mydomain.org',
	'PassW0rd',
	'https://my_central_server.org/v1/projects/2/forms/my_form_about_birds/Submissions',
	'/home/postgres/medias_odk',
	lower(concat(unaccent(replace(user_name,' ','_')),'_',prise_image))
) FROM odk_central.my_form_about_birds_new_data
WHERE image IS NOT NULL;
*/
```



