# Central2PG
## PostgreSQL set of functions to interact with ODK Central's trough its ODATA API for data retrieval and form management (updating)

Fonctions PostgreSQL permettant d'interragir avec un serveur ODK Central à travers son API ODATA, pour la récupération des données et la gestion (mise à jour) de formulaires.

Those functions make use of the "COPY FROM PROGRAM" PostgreSQL capability. The called program is curl. So curl need to be installed on your database server.
Security issues are for the moment bypassed with the use of -k function, considering we know the server called by curl.

central2pg functions need to be installed in the destination database, which will ask central for data.

![central2pg_in_the_data_flow](https://user-images.githubusercontent.com/1642645/165459944-a8bfe56e-6cf3-410d-b337-70fe6d1e5ef3.png)

## How to use it - Example

Simply run [central2pg.sql](https://github.com/mathieubossaert/central2pg/blob/master/central2pg.sql) script in your database after you checked curl is installed.
Functions are created in a schema named "odk_central". Adapt it to your needs.
And start retrieving data from Central.

```sql
SELECT odk_central.odk_central_to_pg(
	'me@mydomain.org',                  -- user
	'PassW0rd',                         -- password
	'my_central_server.org',            -- central FQDN
	2,                                  -- the project id, 
	'my_form_about_birds',              -- form ID
	'odk_data',                         -- schema where to creta tables and store data
	'point_auto,point,ligne,polygone'	-- columns to ignore in json transformation to database attributes (geojson fields of GeoWidgets)
);


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

SELECT odk_central.odk_central_to_pg('me@mydomain.org', 'PassW0rd', 'my_central_server.org', 2, 'my_form_about_birds', 'odk_data','localisation');

/* 
	This is a view build upon generated data tables for our particular needs. It shows only new data (wich are not already in our internal database)
*/

REFRESH MATERIALIZED VIEW odk_central.my_form_about_birds_new_data;

/* 	
	Here we get attachments for new data with attachment.
*/

SELECT outils.get_file_from_central(
	'me@mydomain.org',
	'PassW0rd',
	'my_central_server.org', 
	2, 
	'my_form_about_birds',
	submission_id,
	prise_image,
	'/home/postgres/medias_odk',
	lower(concat(unaccent(replace(user_name,' ','_')),'_',prise_image))
) FROM odk_central.my_form_about_birds_new_data
WHERE image IS NOT NULL;
*/
```

## Short french demo with english subtitles
https://www.youtube.com/watch?v=Z4rY1ejNlW0&t

## Use cases
https://forum.getodk.org/t/odk-to-postgresql-to-nearly-live-webmap/36973

https://forum.getodk.org/t/updating-external-datasets-from-another-forms-submissions-data-from-within-a-postgresql-database/37596?u=mathieubossaert

# pl-pyODK

[pyODK](https://getodk.github.io/pyodk/) offers the possibility to better interact with ODK Central's database.
We are developping new functions that make use of it.

This is a dadicated project you'll find here : [pl-pyODK](https://github.com/mathieubossaert/pl-pyodk)