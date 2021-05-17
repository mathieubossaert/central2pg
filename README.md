# Central2PG
## PostgreSQL's functions to retrieve datas from ODK Central's OData API to a PostgreSQL database 

Fonctions pl/pgsql de récupération des données d'ODK central vers une base de données PostgreSQL

Those functions make use of the "COPY FROM PROGRAM" PostgreSQL capability. The called program is curl. So curl need to be installed on your database server.
Security issues are for the moment bypassed with the use of -k function, considering we know the server called by curl.

## How to use it - Example

```sql
SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
    '__system%%2FsubmissionDate',
	'gt',
	'2020-10-01',
	'odk_central',
	concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.')))))
)
FROM odk_central.get_form_tables_list_from_central('my_email@address.org','my_passw0rd','central.myserver.org',	4,'Sicen');

SELECT odk_central.feed_data_tables_from_central('odk_central',concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.'))))))
FROM odk_central.get_form_tables_list_from_central('my_email@address.org','my_passw0rd','central.myserver.org',	4,'Sicen');

```

## Complete update process from Central
```sql
SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
    '__system%%2FsubmissionDate',
	'gt',
	'2020-10-01',
	'odk_central',
	concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.')))))
)
FROM odk_central.get_form_tables_list_from_central('my_email@address.org','my_passw0rd','central.myserver.org',	4,'Sicen');

SELECT odk_central.feed_data_tables_from_central('odk_central',concat('form_',lower(form),'_',lower(split_part(tablename,'.',cardinality(regexp_split_to_array(tablename,'\.'))))))
FROM odk_central.get_form_tables_list_from_central('my_email@address.org','my_passw0rd','central.myserver.org',	4,'Sicen');

/* 
	This is a view build upon generated data tables for our particular needs
*/
REFRESH MATERIALIZED VIEW odk_central.donnees_formulaire_sicen;

/* 	
	here we get attachments 
*/
SELECT outils.get_file_from_central_api(
	submission_id,
	prise_image,
	'my_email@address.org',
	'my_passw0rd',
	'https://central.myserver.org/v1/projects/4/forms/Sicen/Submissions',
	'/home/postgres/medias_odk',
	lower(concat(unaccent(replace(user_name,' ','_')),'_',prise_image))
) FROM odk_central.donnees_formulaire_sicen
WHERE prise_image IS NOT NULL;

/* 
	And here we ull data from the materialiezd view to our internat tool table, 
	to show ODK Collect data within our web internal tool and also QGIS or Redash
	This function just perform an 
	INSERT INTO table(data_id, col1,col2...)
	SELECT col_a, col_b,... 
	FROM odk_central.donnees_formulaire_sicen 
	LEFT JOIN data_already_there USING(data_id) 
	WHERE data_already_there.data_id IS NULL --to insert only new datas
*/
SELECT odk_central.formulaire_sicen_alimente_saisie_observation_especes();
/* 
```

Functions are created in a schema named "odk_central". Adapt it to your needs.

## "Main" functions

### get_form_tables_list_from_central.sql
#### description
Returns the lists of "table" composing a form. The "core" one and each one corresponding to each repeat_group.
#### parameters : 
	email text				-- the login (email adress) of a user who can get submissions
	password text			-- his password
	central_domain text 	-- ODK Central fqdn : central.mydomain.org
	project_id integer		-- the Id of the project ex. 4
	form_id text			-- the name of the Form ex. Sicen
#### returning : 
	TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablename text)
### get_submission_from_central.sql
#### description
Get json data from Central, feed a temporary table with a generic name central_json_from_central.
Once the temp table is created and filled, PG checks if the destination (permanent) table exists. If not PG creates it with only one json column named "value".
PG does the same to check if a unique constraint on the \_\_id exists. This index will be use to ignore subissions already previously inserted in the table, using an "ON CONFLICT xxx DO NOTHING"
#### parameters : 
	email text						-- the login (email adress) of a user who can get submissions
	password text					-- his password
	central_domain text 			-- ODK Central fqdn : central.mydomain.org
	project_id integer				-- the Id of the project ex. 4
	form_id text					-- the name of the Form ex. Sicen
	form_table_name text			-- the table of the form to get value from (one of thoses returned by get_form_tables_list_from_central() function
	column_to_filter text			-- the column (__system/submitterId or __system/submissionDate  on wich you want to apply a filter (only works on Submissions table
	filter text						-- the filter to apply (gt = greater than, lt = lower than)
	filter_value text				-- the value to compare the column with
	destination_schema_name text 	-- the name of the schema where to create the permanent table 
	destination_table_name text		-- the name of this table 
#### returning : 
	void
### feed_data_tables_from_central.sql
#### description
#### parameters :
	schema_name text	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
	table_name text	-- the table containing plain json submission from the get_submission_from_central() function call
#### returning :
	void
### get_file_from_central_api.sql
#### description
Download each media mentioned in submissions
#### parameters : 
	email text				-- the login (email adress) of a user who can get submissions
	password text			-- his password
	central_domain text 	-- ODK Central fqdn : central.mydomain.org
	project_id integer		-- the Id of the project ex. 4
	form_id text			-- the name of the Form ex. Sicen
	submission_id text
	image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
	destination text		-- Where you want curl to store the file (path to directory)
	output text				-- filename with extension
#### returning : 
	void
## "Shadow" functions needed

### dynamic_pivot.sql
#### description
-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html (thanks again)
CREATE a pivot table dynamically, withut specifying mannually the row structure.
Returns a cursor use by both following finction to create a table and feed it
#### parameters : 
	central_query text 	-- the query defining the data
	headers_query text		-- the query defining the columns
	INOUT cname refcursor	-- the name of the cursor
#### returning : 
	refcursor
### create_table_from_refcursor.sql
#### description
-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
Create a table corresponding to the curso structure (attribute types and names)
#### parameters : 
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
#### returning : 
	void
### insert_into_from_refcursor.sql
#### description
-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
Feed the table with data
#### parameters : 
	_table_name text, 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
#### returning : 
	void
