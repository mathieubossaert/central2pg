/*
Change schema name odk_central on two firts lines to what you want
And adapt each occurence of "SET search_path=odk_central,public;" with the schema you choose"
*/


CREATE SCHEMA IF NOT EXISTS odk_central;


/*
TABLE: central_authentication_tokens(text, text, text)

	description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		username text 						-- Central username
		password text 						-- Username's password
		project_id integer					-- project_id
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens

*/

CREATE TABLE IF NOT EXISTS odk_central.central_authentication_tokens
(
    url text COLLATE pg_catalog."default" NOT NULL,
    username text,
    password text,
    project_id integer,
    central_token text COLLATE pg_catalog."default",
    expiration timestamp with time zone,
    CONSTRAINT central_authentication_tokens_pkey PRIMARY KEY (url)
);

COMMENT ON TABLE  odk_central.central_authentication_tokens 
	IS 'description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		username text 						-- Central username
		password text 						-- Username''s password
		project_id integer					-- project_id
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens';


/*
FUNCTION: get_fresh_token_from_central(text, text, text)

	description :
		Ask central for a new fresh token for the given Central server with given login and password. And update the database token table with it.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_fresh_token_from_central(
	email text,
	password text,
	central_domain text)
    RETURNS TABLE(url text, central_token text, expiration timestamp with time zone) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare url text;
declare requete text;
BEGIN
requete = concat('curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d ''''{"email":"',email,'","password":"',password,'"}'''' https://',central_domain,'/v1/sessions');

EXECUTE (
		'DROP TABLE IF EXISTS central_token;
		 CREATE TEMP TABLE central_token(form_data json);'
		);

EXECUTE format('COPY central_token FROM PROGRAM '''||requete||''' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
RETURN QUERY EXECUTE 
FORMAT('INSERT INTO odk_central.central_authentication_tokens(url, central_token, expiration)
	   SELECT '''||central_domain||''' as url, form_data->>''token'' as central_token, (form_data->>''expiresAt'')::timestamp with time zone as expiration FROM central_token 
	   ON CONFLICT(url) DO UPDATE SET central_token = EXCLUDED.central_token, expiration = EXCLUDED.expiration
	   RETURNING  url, central_token, expiration;');
END;
$BODY$;

COMMENT ON FUNCTION  odk_central.get_fresh_token_from_central(text,text,text)
	IS 'description :
		Ask central for a new fresh token for the given Central server with given login and password. And update the database token table with it.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN
	
	returning :
		void'
;/*
FUNCTION: get_token_from_central(text, text, text)	

	description :
		Return a valid token, from the database id it exists and is still valid, or ask a new one (calling get_fresh_token_from_central(text,texttext) function) from ODK Central and then update the token table in the database.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central 
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_token_from_central(
	_email text,
	_password text,
	_central_domain text)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
WITH tokens AS (SELECT url, central_token, expiration
	FROM odk_central.central_authentication_tokens
	WHERE url = _central_domain
	UNION 
	SELECT _central_domain,null,'1975-12-01'::timestamp with time zone),
more_recent_token AS (
SELECT url, central_token, expiration
FROM tokens ORDER BY expiration DESC LIMIT 1)
SELECT CASE 
	WHEN expiration >= now()::timestamp with time zone THEN central_token 
	ELSE (Select central_token FROM odk_central.get_fresh_token_from_central(_email, _password, _central_domain)) 
END as jeton 
	   FROM more_recent_token
$BODY$;

COMMENT ON FUNCTION  odk_central.get_token_from_central(text,text,text)
	IS 'description :
		Return a valid token, from the database id it exists and is still valid, or ask a new one (calling get_fresh_token_from_central(text,texttext) function) from ODK Central and then update the token table in the database.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central 
	
	returning :
		void'
;


/*
FUNCTION: dynamic_pivot(text, text, refcursor)
	description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		Creates a pivot table dynamically, without specifying mannually the row structure.
		Returns a cursor use by both following function to create a table and fill it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor
*/

CREATE OR REPLACE FUNCTION odk_central.dynamic_pivot(
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

COMMENT ON FUNCTION odk_central.dynamic_pivot(text, text,refcursor) IS 'description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		Creates a pivot table dynamically, without specifying mannually the row structure.
		Returns a cursor use by both following function to create a table and fill it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor';


/*
FUNCTION: does_index_exists(text, text)
	description : 
	checks if a unique index already exists on form_data ->> '__id'
	
	parameters :
	schemaname text 		-- the name of the schema
	tablename text		-- the name of the table
	
	returning :
	boolean
*/

CREATE OR REPLACE FUNCTION odk_central.does_index_exists(
	schemaname text, 
	tablename text)
	RETURNS boolean AS 
	$BODY$
		SELECT count(indexname)>0 AS nb_indexes
		FROM pg_indexes
		WHERE schemaname = $1 and tablename = $2
		AND (indexdef ILIKE '%form_data ->> ''__id''%' OR indexdef ILIKE '%USING btree (data_id)%')
	$BODY$ 
LANGUAGE SQL;

COMMENT ON function odk_central.does_index_exists(text,text) IS 'description : 
	checks if a unique index already exists on form_data ->> ''__id''
	
	parameters :
	schemaname text 		-- the name of the schema
	tablename text		-- the name of the table
	
	returning :
	boolean';


/*
FUNCTION: create_table_from_refcursor(text, refcursor)
	description : 
	-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Create a table corresponding to the cursor structure (attribute types and names). As json atributes are not typed, all attributes are created as text ones.
	You'll need to cast each in your subsequent requests.
	
	parameters :
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void
*/

CREATE OR REPLACE FUNCTION odk_central.create_table_from_refcursor(
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
          
-- RAISE INFO 'SQL script for table cration %',_sql; 
    EXECUTE (_sql);
  _sql_index = 'CREATE UNIQUE INDEX IF NOT EXISTS idx_'||left(md5(random()::text),20)||' ON '||_schema_name||'.'||_table_name||' USING btree ("data_id")    TABLESPACE pg_default;';
    
	IF odk_central.does_index_exists(_schema_name,_table_name) IS FALSE THEN
    EXECUTE (_sql_index);
	END IF;	
	
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
-- RAISE INFO 'SQL script for new cols %',_sql_new_cols; 
    EXECUTE (COALESCE(_sql_new_cols,'SELECT true;')); 
 RAISE INFO 'exiting from  create_table_from_refcursor() for table %',_table_name; 
 RAISE INFO 'create_table_from_refcursor(): SQL statement is: %', COALESCE(_sql_new_cols,'no new column to add');
END;
$BODY$;
COMMENT ON function odk_central.create_table_from_refcursor(text,text,refcursor) IS 'description : 
	-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Create a table corresponding to the cursor structure (attribute types and names). As json atributes are not typed, all attributes are created as text ones.
	You''ll need to cast each in your subsequent requests.
	
	parameters :
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void';


/*
FUNCTION: insert_into_from_refcursor(text, text, refcursor)	

	description :
		-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
		Fills the table with data
	
	parameters :
		_schema_name text, 		-- the name of the schema where to create the table
		_table_name text, 		-- the name of the table to create
		_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.insert_into_from_refcursor(
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

COMMENT ON function odk_central.insert_into_from_refcursor(text,text,refcursor)IS '	
	description :
	-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Fills the table with data
	
	parameters :
		_schema_name text, 		-- the name of the schema where to create the table
		_table_name text, 		-- the name of the table to create
		_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void';	


/*
FUNCTION: get_form_tables_list_from_central(text, text, text, integer, text)
	description :
		Returns the lists of "tables" composing a form. The "core" one and each one corresponding to each repeat_group.
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central FQDN : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
	
	returning :
		TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablepath text, tablename text)
*/

CREATE OR REPLACE FUNCTION odk_central.get_form_tables_list_from_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text)
    RETURNS TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablepath text, tablename text) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare url text;
declare requete text;
BEGIN
url = replace(concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'.svc'),' ','%%20');

EXECUTE format('DROP TABLE IF EXISTS central_json_from_central;
			   CREATE TEMP TABLE central_json_from_central(form_data json);'
		);

EXECUTE format('COPY central_json_from_central FROM PROGRAM $$ curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET "'||url||'" -H "Accept: application/json" -H ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' $$ CSV QUOTE E''\x01'' DELIMITER E''\x02'';');

RETURN QUERY EXECUTE 
FORMAT('WITH data AS (SELECT json_array_elements(form_data -> ''value'') AS form_data FROM central_json_from_central)
	   SELECT '''||email||''' as user_name, 
	   '''||password||''' as pass_word, 
	   '''||central_domain||''' as central_fqdn, 
	   '||project_id||' as project, 
	   '''||form_id||''' as form, 
	   (form_data ->> ''name'') AS table_path , 
	   (form_data ->> ''name'') AS tablename
	   FROM data;');
END;
$BODY$;

ALTER FUNCTION odk_central.get_form_tables_list_from_central(text, text, text, integer, text)
    OWNER TO dba;

COMMENT ON FUNCTION odk_central.get_form_tables_list_from_central(text, text, text, integer, text)
    IS 'description :
		Returns the lists of "table" composing a form. The "core" one and each one corresponding to each repeat_group.
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
	
	returning :
		TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablepath text, tablename text)';


/*
FUNCTION: get_submission_from_central(text, text, text, integer, text, text, text, text, text, text, text)
	description
		Get json data from Central, feed a temporary table with a generic name central_json_from_central.
		Once the temp table is created and filled, PG checks if the destination schema and (permanent) table exist. If not PG creates it with only one json column named "value".
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
*/

CREATE OR REPLACE FUNCTION odk_central.get_submission_from_central(
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
url = replace(concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'.svc/',form_table_name),' ','%%20');
EXECUTE (
		'DROP TABLE IF EXISTS central_json_from_central;
		 CREATE TEMP TABLE central_json_from_central(form_data json);'
		);

EXECUTE format('COPY central_json_from_central FROM PROGRAM $$ curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET "'||url||'" -H "Accept: application/json" -H ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' $$ CSV QUOTE E''\x01'' DELIMITER E''\x02'';');

EXECUTE format('CREATE SCHEMA IF NOT EXISTS '||destination_schema_name||';
CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||destination_table_name||' (form_data json);');
	IF odk_central.does_index_exists(destination_schema_name,destination_table_name) IS FALSE THEN
		EXECUTE format ('CREATE UNIQUE INDEX IF NOT EXISTS idx_'||left(md5(random()::text),20)||'
		ON '||destination_schema_name||'.'||destination_table_name||' USING btree
		((form_data ->> ''__id''::text) COLLATE pg_catalog."default" ASC NULLS LAST)
		TABLESPACE pg_default;');
	END IF;	
EXECUTE format('INSERT into '||destination_schema_name||'.'||destination_table_name||'(form_data) SELECT json_array_elements(form_data -> ''value'') AS form_data FROM central_json_from_central ON CONFLICT ((form_data ->> ''__id''::text)) DO NOTHING;');
END;
$BODY$;

COMMENT ON FUNCTION  odk_central.get_submission_from_central(text,text,text,integer,text,text,text,text)
	IS 'description :
		Get json data from Central, feed a temporary table with a generic name central_json_from_central.
		Once the temp table is created and filled, PG checks if the destination schema and (permanent) table exists. If not PG creates it with only one json column named "value".
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
	future version should use filters... With more parameters';


/*
FUNCTION: feed_data_tables_from_central(text, text)

	description : 
		Feed the tables from key/pair tables. 
	
	parameters :
		schema_name text		-- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text			-- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text 	-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void

*/

CREATE OR REPLACE FUNCTION odk_central.feed_data_tables_from_central(
	schema_name text,
	table_name text,
	geojson_columns text
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
--declare keys_to_ignore text;
declare non_empty boolean;
BEGIN

EXECUTE format('SELECT exists(select 1 FROM %1$s.%2$s)', schema_name, table_name)
INTO non_empty;

IF non_empty THEN 
RAISE INFO 'entering feed_data_tables_from_central for table %', table_name; 
EXECUTE format('DROP TABLE IF EXISTS data_table;
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
				
EXECUTE format('SELECT odk_central.dynamic_pivot(''SELECT data_id, key, value FROM data_table ORDER BY 1,2'',''SELECT DISTINCT key FROM data_table ORDER BY 1'',''curseur_central'');
			   		SELECT odk_central.create_table_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
			   		MOVE BACKWARD FROM "curseur_central";
			   		SELECT odk_central.insert_into_from_refcursor('''||schema_name||''','''||table_name||'_data'', ''curseur_central'');
				   	CLOSE "curseur_central"'
			  );	
RAISE INFO 'exiting from feed_data_tables_from_central for table %', table_name; 
ELSE
	RAISE INFO 'table % is empty !', table_name; 
END IF;
END;
$BODY$;

COMMENT ON FUNCTION odk_central.feed_data_tables_from_central(text,text,text)
IS 'description : 
		Feed the tables from key/pair tables.

	parameters :
		schema_name text	 -- the schema where is the table containing plain json submission from the get_submission_from_central() function call
		table_name text		 -- the table containing plain json submission from the get_submission_from_central() function call
		geojson_columns text -- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone''... depending on your question names
	
	returning :
		void';


/*
FUNCTION: get_form_tables_list_from_central(text, text, text, integer, text, text, text, text, text)
	description :
		Download each media mentioned in submissions
	
	parameters :
		email text              -- the login (email adress) of a user who can get submissions
		password text           -- his password
		central_domain text     -- ODK Central fqdn : central.mydomain.org
		project_id integer      -- the Id of the project ex. 4
		form_id text            -- the name of the Form ex. Sicen
		submission_id text      -- the submission_id
		image text              -- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text        -- Where you want curl to store the file (path to directory)
		output text             -- filename with extension
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_file_from_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	submission_id text,
	image text,
	destination text,
	output text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
BEGIN
url = replace(concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/Submissions/',submission_id,'/attachments/',image),' ','%%20');
EXECUTE format('DROP TABLE IF EXISTS central_media_from_central;');
EXECUTE format('CREATE TEMP TABLE central_media_from_central(reponse text);');
EXECUTE format('COPY central_media_from_central FROM PROGRAM $$ curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET '||url||' -o '||destination||'/'||output||' -H "Accept: application/json" -H ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' $$ ;');
END;
$BODY$;

COMMENT ON FUNCTION odk_central.get_file_from_central(text, text, text, integer, text, text, text, text, text)
    IS 'description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text		-- the submission_id
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void';


/*
FUNCTION: odk_central_to_pg(text, text, text, integer, text, text)

	description :
		Wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\
	
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
	central_FQDN,
	project,
	form,
	tablename,
	'''||destination_schema_name||''',
	lower(trim(regexp_replace(left(concat(''form_'',form,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_''))
	)
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');

EXECUTE format('
SELECT odk_central.feed_data_tables_from_central(
	'''||destination_schema_name||''',lower(trim(regexp_replace(left(concat(''form_'',form,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_'')),'''||geojson_columns||''')
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');
END;
$BODY$;

COMMENT ON FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text, text)
    IS 'description :
		wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\
	
	returning :
		void';


/*
FUNCTION: get_form_version(text, text, text, integer, text)
	description
		Asks central for the current version of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_form_version(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare current_version text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id);
EXECUTE (
		'DROP TABLE IF EXISTS form_version;
		 CREATE TEMP TABLE form_version(form_data json);'
		);
EXECUTE format('COPY form_version FROM PROGRAM $$ curl --insecure --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' '''||url||''' $$ ;');
SELECT form_data->>'version' INTO current_version FROM form_version;
RETURN current_version;
END;
$BODY$;


COMMENT ON FUNCTION odk_central.get_form_version(text, text, text, integer, text)
    IS '
	description
		Asks central for the current version of the given form. Returns it as a text.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		text';


/*
FUNCTION: create_draft(text, text, text, integer, text)
	description
		Creates a new draft of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.create_draft(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft?ignoreWarnings=true');
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl  --insecure --include --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' --header "Content-Type:" --data-binary "" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION odk_central.create_draft(text, text, text, integer, text)
    IS 'description :
		Creates a new draft of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void';


/*
FUNCTION: push_media_to_central(text, text, text, integer, text, text, text)
	description
		Pushes the given file as an attachment of the current draft of the given form to Central.
		The function checks the file extension and adapt the content type header of the curl command.
		
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		media_path text					-- the path where the file can be find
		media_name text					-- the name of the file with its extension (xml or geojson or csv)
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.push_media_to_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	media_path text,
	media_name text -- must end by .xml or .geojson or .csv
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare content_type text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft/attachments/',media_name);
content_type = CASE reverse(split_part(reverse(media_name),'.',1)) -- to be sure to get string after last point in the filename (if other were used : toto.2022.xml)
	WHEN 'csv' THEN 'text.csv'
	WHEN 'geojson' THEN 'application/geojson'
	WHEN 'xml' THEN 'application/xml'
END;
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' --header "Content-Type: '||content_type||'" --data-binary "@'||media_path||'/'||media_name||'" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION odk_central.push_media_to_central(text, text, text, integer, text, text, text)
    IS 'description
		Pushes the given file as an attachment of the current draft of the given form to Central
		The function checks the file extension and adapt the content type header of the curl command.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		media_path text					-- the path where the file can be find
		media_name text					-- the name of the file with its extension (xml or geojson or csv)
	
	returning :
		void';


/*
FUNCTION: publish_form_version(text, text, text, integer, text, integer)

	description
		Publishes the current draft of the given form with the given version number.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		version_number integer			-- the new version number to use
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.publish_form_version(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	version_number integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft/publish?version=',version_number);
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --include --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' '''||url||''' $$ ;');

END;
$BODY$;


COMMENT ON FUNCTION odk_central.publish_form_version(text, text, text, integer, text, integer)
    IS '
	description
		Publishes the current draft of the given form with the given version number.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		version_number integer			-- the new version number to use
	
	returning :
		void';