/*
   change schema name odk_central
   on two firts lines to what you want
Do the same for "SET search_path=odk_central;" statement in the feed_data_tables_from_central(text,text) function
*/
CREATE SCHEMA IF NOT EXISTS odk_central;
SET SEARCH_PATH TO odk_central;


/*
FUNCTION: dynamic_pivot(text, text, refcursor)
	description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		CREATE a pivot table dynamically, withut specifying mannually the row structure.
		Returns a cursor use by both following finction to create a table and feed it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor
*/

CREATE OR REPLACE FUNCTION dynamic_pivot(
	central_query text,
	headers_query text,
	INOUT cname refcursor DEFAULT NULL::refcursor)
    RETURNS refcursor
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  left_column text;
  header_column text;
  value_column text;
  h_value text;
  headers_clause text;
  query text;
  j json;
  r record;
  i int:=1;
BEGIN
  -- find the column names of the source query
  EXECUTE 'select row_to_json(_r.*) from (' ||  central_query || ') AS _r' into j;
  FOR r in SELECT * FROM json_each_text(j)
  LOOP
    IF (i=1) THEN left_column := r.key;
      ELSEIF (i=2) THEN header_column := r.key;
      ELSEIF (i=3) THEN value_column := r.key;
    END IF;
    i := i+1;
  END LOOP;

  -- build the dynamic transposition quer, based on the canonical model
  -- (CASE WHEN...)
  FOR h_value in EXECUTE headers_query
  LOOP
    headers_clause := concat(headers_clause,
     format(chr(10)||',min(case when %I=%L then %I::text end) as %I',
           header_column,
	   h_value,
	   value_column,
	   h_value ));
  END LOOP;

  query := format('SELECT %I %s FROM (select *,row_number() over() as rn from (%s) AS _c) as _d GROUP BY %I order by min(rn)',
           left_column,
	   headers_clause,
	   central_query,
	   left_column);

  -- open the cursor so the caller can FETCH right away.
  -- if cname is not null it will be used as the name of the cursor,
  -- otherwise a name "<unnamed portal unique-number>" will be generated.
  OPEN cname FOR execute query;
END
$BODY$;

COMMENT ON FUNCTION dynamic_pivot(text, text,refcursor) IS 'description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		CREATE a pivot table dynamically, withut specifying mannually the row structure.
		Returns a cursor use by both following finction to create a table and feed it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor';/*
FUNCTION: create_table_from_refcursor(text, refcursor)
	description : 
	-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Create a table corresponding to the curso structure (attribute types and names)
	
	parameters :
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void
*/

CREATE OR REPLACE FUNCTION create_table_from_refcursor(
	_schema_name text,
	_table_name text,
	_ref refcursor)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  _sql       text;
  _sql_index       text;
  _sql_val   text = '';
  _sql_existing_cols   text = '';
  _sql_new_cols   text = '';
  _row       record;
BEGIN
 RAISE INFO 'entering create_table_from_refcursor() for table %',_table_name; 
    FETCH FIRST FROM _ref INTO _row;
    SELECT _sql_val || '
           (' ||
           STRING_AGG(concat('"',val.key :: text,'" text'), ',') ||
           ')'
        INTO _sql_val
    FROM JSON_EACH(TO_JSON(_row)) val;
  _sql = '
          CREATE TABLE IF NOT EXISTS ' || _schema_name ||'.'|| _table_name || '
          ' || _sql_val;
    EXECUTE (_sql);
  _sql_index = 'CREATE UNIQUE INDEX IF NOT EXISTS idx_'||replace(_table_name,'.','_')||' ON '||_schema_name||'.'||_table_name||' USING btree ("data_id")
    TABLESPACE pg_default;';
    EXECUTE (_sql_index);
	
	/* ading new columns */
	SELECT _sql_new_cols || 
           STRING_AGG(concat('ALTER TABLE ' , _schema_name ,'.', _table_name , ' ADD COLUMN "',val.key :: text,'" text'), ';') ||';'
        INTO _sql_new_cols
    FROM JSON_EACH(TO_JSON(_row)) val
	WHERE val.key NOT IN ( SELECT attname 
 FROM pg_class JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid
 JOIN pg_namespace ON relnamespace = pg_namespace.oid
 WHERE nspname = _schema_name
   AND relkind = 'r' AND pg_class.relname = _table_name AND attnum > 0 AND attname = val.key
);
	-- Create new attributes or Run a dummy query if nothing new
    EXECUTE (COALESCE(_sql_new_cols,'SELECT true;')); 
 RAISE INFO 'exiting from  create_table_from_refcursor() for table %',_table_name; 
 RAISE INFO 'create_table_from_refcursor(): SQL statement is: %', COALESCE(_sql_new_cols,'no new column to add');
END;
$BODY$;
COMMENT ON function create_table_from_refcursor(text,text,refcursor) IS 'description : 
	-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Create a table corresponding to the curso structure (attribute types and names)
	
	parameters :
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void';/*
FUNCTION: insert_into_from_refcursor(text, text, refcursor)	
	description :
	-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Feed the table with data
	
	parameters :
	_schema_name text, 		-- the name of the schema where to create the table
	_table_name text, 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void
*/

CREATE OR REPLACE FUNCTION insert_into_from_refcursor(
	_schema_name text,
	_table_name text,
	_ref refcursor)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
  _sql       text;
  _sql_val   text = '';
  _sql_col   text = '';
  _row       record;
  _hasvalues boolean = FALSE;
BEGIN

  LOOP   --for each row
    FETCH _ref INTO _row;
    EXIT WHEN NOT found;   --there are no rows more

    SELECT _sql_val || '
           (' ||
           STRING_AGG(
			   concat(
				   CASE WHEN val.value::text='null' OR val.value::text='' OR val.value::text='\null' OR val.value::text='"null"'
				   THEN 'null'
				   ELSE 
				   concat('''',replace(trim(val.value :: text,'\"'),'''',''''''),'''')
				   END)
			   , ',' ORDER BY val.key) ||
           '),'
        INTO _sql_val
    FROM JSON_EACH(TO_JSON(_row)) val;

    SELECT _sql_col || STRING_AGG(concat('"',val.key :: text,'"'), ',' ORDER BY val.key) 
        INTO _sql_col
    FROM JSON_EACH(TO_JSON(_row)) val;

  _sql_val = TRIM(TRAILING ',' FROM _sql_val);
  _sql_col = TRIM(TRAILING ',' FROM _sql_col);
  _sql = '
          INSERT INTO ' || _schema_name || '.' || _table_name || '(' || _sql_col || ')
          VALUES ' || _sql_val ||' ON CONFLICT (data_id) DO NOTHING;';
	
	EXECUTE (_sql);
	_sql_val = '';
	_sql_col = '';
  END LOOP;
  
  --RAISE NOTICE 'insert_into_from_refcursor(): SQL is: %', _sql;

END;
$BODY$;

COMMENT ON function insert_into_from_refcursor(text,text,refcursor)IS '	
	description :
	-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Feed the table with data
	
	parameters :
	_schema_name text, 		-- the name of the schema where to create the table
	_table_name text, 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void
	
-> is adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381';/*
FUNCTION: get_form_tables_list_from_central(text, text, text, integer, text)
	description :
		Returns the lists of "table" composing a form. The "core" one and each one corresponding to each repeat_group.
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
	
	returning :
		TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablename text)
*/

CREATE OR REPLACE FUNCTION get_form_tables_list_from_central(
	email text,				
	password text,			
	central_domain text, 	
	project_id integer,		
	form_id text			
	)
    RETURNS TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablename text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'.svc');
EXECUTE (
		'DROP TABLE IF EXISTS central_json_from_central;
		 CREATE TEMP TABLE central_json_from_central(form_data json);'
		);
EXECUTE format('COPY central_json_from_central FROM PROGRAM ''curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 --user "'||email||':'||password||'" "'||url||'"'' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
RETURN QUERY EXECUTE 
FORMAT('WITH data AS (SELECT json_array_elements(form_data -> ''value'') AS form_data FROM central_json_from_central)
	   SELECT '''||email||''' as user_name, '''||password||''' as pass_word, '''||central_domain||''' as central_fqdn, '||project_id||' as project, '''||form_id||''' as form, (form_data ->> ''name'') AS table_name FROM data;');
END;
$BODY$;

COMMENT ON FUNCTION get_form_tables_list_from_central(text, text, text, integer, text) IS 'description :
		Returns the lists of "table" composing a form. The "core" one and each one corresponding to each repeat_group.
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
	
	returning :
		TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablename text)';/*
FUNCTION: get_submission_from_central(text, text, text, integer, text, text, text, text, text, text, text)
	description
		Get json data from Central, feed a temporary table with a generic name central_json_from_central.
		Once the temp table is created and filled, PG checks if the destination (permanent) table exists. If not PG creates it with only one json column named "value".
		PG does the same to check if a unique constraint on the __id exists. This index will be use to ignore subissions already previously inserted in the table, using an "ON CONFLICT xxx DO NOTHING"
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		form_table_name text			-- the table of the form to get value from (one of thoses returned by get_form_tables_list_from_central() function
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		destination_table_name text		-- the name of this table 
	
	returning :
		void

	comment : 	
	future version should use filters... With more parameters
	Waiting for centra next release (probably May 2021)
*/

CREATE OR REPLACE FUNCTION get_submission_from_central(
	email text,						
	password text,					
	central_domain text, 			
	project_id integer,				
	form_id text,					
	form_table_name text,			
	destination_schema_name text, 	
	destination_table_name text		
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'.svc/',form_table_name);
EXECUTE (
		'DROP TABLE IF EXISTS central_json_from_central;
		 CREATE TEMP TABLE central_json_from_central(form_data json);'
		);
EXECUTE format('COPY central_json_from_central FROM PROGRAM ''curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 --user "'||email||':'||password||'" "'||url||'"'' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
EXECUTE format('CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||destination_table_name||' (form_data json);');
EXECUTE format ('CREATE UNIQUE INDEX IF NOT EXISTS id_idx_'||destination_table_name||'
    ON '||destination_schema_name||'.'||destination_table_name||' USING btree
    ((form_data ->> ''__id''::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;');
EXECUTE format('INSERT into '||destination_schema_name||'.'||destination_table_name||'(form_data) SELECT json_array_elements(form_data -> ''value'') AS form_data FROM central_json_from_central ON CONFLICT ((form_data ->> ''__id''::text)) DO NOTHING;');
END;
$BODY$;

COMMENT ON FUNCTION  get_submission_from_central(text,text,text,integer,text,text,text,text)
	IS 'description :
		Get json data from Central, feed a temporary table with a generic name central_json_from_central.
		Once the temp table is created and filled, PG checks if the destination (permanent) table exists. If not PG creates it with only one json column named "value".
		PG does the same to check if a unique constraint on the __id exists. This index will be use to ignore subissions already previously inserted in the table, using an "ON CONFLICT xxx DO NOTHING"
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		form_table_name text			-- the table of the form to get value from (one of thoses returned by get_form_tables_list_from_central() function
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		destination_table_name text		-- the name of this table 
	
	returning :
		void

	comment : 	
	future version should use filters... With more parameters
	Waiting for centra next release (probably May 2021)';/*
FUNCTION: feed_data_tables_from_central(text, text)

!!! You need to edit and set the correct search_path at the beginig of the EXECUTE statement : SET search_path=odk_central; Replace "odk_central" by the name of the schema where you created the functions !!!

	description : 
		Feed the tables from key/pair tables. 
	parameters :
		schema_name text	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text		-- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void

*/

CREATE OR REPLACE FUNCTION feed_data_tables_from_central(
	schema_name text,	-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
	table_name text,	-- the table containing plain json submission from the get_submission_from_central() function call
	geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like 'point,polygon'... depending on your question names ex. : 'geopoint_widget_placementmap,point,ligne,polygone'
    )
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
--declare keys_to_ignore text;
BEGIN

RAISE INFO 'entering feed_data_tables_from_central for table %', table_name; 
EXECUTE format('SET search_path=odk_central,public;
	DROP TABLE IF EXISTS data_table;
	CREATE TABLE data_table(data_id text, key text, value json);
	INSERT INTO  data_table(data_id, key, value) 
	WITH RECURSIVE doc_key_and_value_recursive(data_id, key, value) AS (
	  SELECT
		('||table_name||'.form_data ->> ''__id'') AS data_id,
		t.key,
		t.value
	  FROM '||schema_name||'.'||table_name||', json_each('||table_name||'.form_data) AS t
	  UNION ALL
	  SELECT
		doc_key_and_value_recursive.data_id,
		t.key,
		t.value
	  FROM doc_key_and_value_recursive,
		json_each(CASE 
		  WHEN json_typeof(doc_key_and_value_recursive.value) <> ''object'' OR key = ANY(string_to_array('''||geojson_columns||''','','')) THEN ''{}'' :: JSON
		  ELSE doc_key_and_value_recursive.value
		END) AS t
	)SELECT data_id, key, value FROM doc_key_and_value_recursive WHERE json_typeof(value) <> ''object'' OR key = ANY(string_to_array('''||geojson_columns||''','','')) ORDER BY 2,1;'
);
				
EXECUTE format('SELECT dynamic_pivot(''SELECT data_id, key, value FROM data_table ORDER BY 1,2'',''SELECT DISTINCT key FROM data_table ORDER BY 1'',''curseur_central'');
			   		SELECT create_table_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
			   		MOVE BACKWARD FROM "curseur_central";
			   		SELECT insert_into_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
				   	CLOSE "curseur_central"'
			  );	
RAISE INFO 'exiting from feed_data_tables_from_central for table %', table_name; 

END;
$BODY$;

COMMENT ON FUNCTION feed_data_tables_from_central(text,text,text)
IS 'description : 
		Feed the tables from key/pair tables. 
	parameters :
		schema_name text	 -- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text		 -- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void
		
	comment :
		Should accept a "keys_to_ignore" parameter (as for geojson fields we want to keep as geojson).
		For the moment the function is specific to our naming convention (point, ligne, polygone)';/*
FUNCTION: get_file_from_central(text, text, text, integer, text, text, text, text, text)
	description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION get_file_from_central(
	email text,				
	password text,			
	central_domain text, 	
	project_id integer,		
	form_id text,			
	submission_id text,     
	image text,				
	destination text,		
	output text				
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/Submissions/',submission_id,'/attachments/',image);
EXECUTE format('DROP TABLE IF EXISTS central_media_from_central;');
EXECUTE format('CREATE TEMP TABLE central_media_from_central(reponse text);');
EXECUTE format('COPY central_media_from_central FROM PROGRAM ''curl --insecure --max-time 30 --user "'||email||':'||password||'" -o '||destination||'/'||output||' "'||url||'"'';');
END;
$BODY$;

COMMENT ON FUNCTION get_file_from_central(text, text, text, integer, text, text, text, text, text) IS 'description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void';/*
FUNCTION: odk_central_to_pg(text, text, text, integer, text, text)
	description
		Retrieve all data from a given form to postgresql tables in the destination_schema.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.odk_central_to_pg(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	destination_schema_name text,
	geojson_columns text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
EXECUTE format('SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
	'''||destination_schema_name||''',
	trim(left(concat(''form_'',lower(replace(form,''-'',''_'')),''_'',lower(split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.''))))),58),''_'')
	)
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');

EXECUTE format('
SELECT odk_central.feed_data_tables_from_central(
	'''||destination_schema_name||''',trim(left(concat(''form_'',lower(replace(form,''-'',''_'')),''_'',lower(split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.''))))),58),''_''),'''||geojson_columns||''')
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');
END;
$BODY$;

ALTER FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text, text)
    OWNER TO dba;

COMMENT ON FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text, text)
    IS 'description :
		wrap the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void
';