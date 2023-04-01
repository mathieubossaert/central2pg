CREATE SCHEMA IF NOT EXISTS plpyodk;
CREATE SCHEMA IF NOT EXISTS odk_central;
CREATE OR REPLACE PROCEDURAL LANGUAGE plpython3u;

-- FUNCTION: plpyodk.get_filtered_complete_submissions(text, text, text)

-- DROP FUNCTION IF EXISTS plpyodk.get_filtered_complete_submissions(text, text, text);
CREATE OR REPLACE FUNCTION plpyodk.get_filtered_complete_submissions(
	project_id text,
	form_id text,
	filter text)
    RETURNS text
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
def fresh_data_only(pid, fid, path, filter, datas):
    from pyodk.client import Client
    import re
    import json 
    client = Client()
    client.open()
    if path == '':
        return None

    url = 'projects/'+pid+'/forms/'+fid+'.svc/'+path+'?$filter='+filter
    if re.match(r"Submissions\?.*", path) or re.match(r".*\)$", path):
        tablename = 'submissions'
    else:
        tablename = path.rsplit('/')[-1]    
    
    response = client.get(url)
    
    value = response.json()['value']
    
    navigationlinks = re.findall(r'(\'\w+@odata\.navigationLink\'):\s+([^\}]+)', str(value))
    for (key, link) in navigationlinks:
        link = link.replace("'", "'").replace('"','')
        fresh_data_only(project_id, form_id, link, '', datas)
    
    if tablename in datas.keys():
        datas[tablename] += value
    else:
        datas[tablename]=value
		
    json_datas = str(json.dumps(datas, indent = 4))
    return json_datas
    #return json_datas.encode(encoding='utf-8').decode('unicode_escape')
	
return fresh_data_only(project_id, form_id, 'Submissions', filter, datas = {})

$BODY$;

-- FUNCTION: plpyodk.get_attachment_from_central(text, text, text, text, text)

-- DROP FUNCTION IF EXISTS plpyodk.get_attachment_from_central(text, text, text, text, text);

CREATE OR REPLACE FUNCTION plpyodk.get_attachment_from_central(
	project_id text,
	form_id text,
	submission_id text,
	attachment text,
	destination text)
    RETURNS text
    LANGUAGE 'plpython3u'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
def save_attachment_to_file(pid, fid, sid, attachment, dest_path):
    from pyodk.client import Client

    client = Client()
    client.open()

    url ='projects/'+pid+'/forms/'+fid+'/Submissions/'+sid+'/attachments/'+attachment
    print(url)
    response = client.get(url)

    with open(dest_path, "wb") as out_file:
        out_file.write(response.content)
    
save_attachment_to_file(project_id,	form_id, submission_id,	attachment,	destination)

$BODY$;

-- FUNCTION: plpyodk.dynamic_pivot(text, text, refcursor)

-- DROP FUNCTION IF EXISTS plpyodk.dynamic_pivot(text, text, refcursor);

CREATE OR REPLACE FUNCTION plpyodk.dynamic_pivot(
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

COMMENT ON FUNCTION plpyodk.dynamic_pivot(text, text, refcursor)
    IS 'description :
		-> adapted from https://postgresql.verite.pro/blog/2018/06/19/crosstab-pivot.html
		Creates a pivot table dynamically, without specifying mannually the row structure.
		Returns a cursor use by both following function to create a table and fill it
	
	parameters :
		central_query text 	-- the query defining the data
		headers_query text		-- the query defining the columns
		INOUT cname refcursor	-- the name of the cursor
	
	returning :
		refcursor';



-- FUNCTION: plpyodk.create_table_from_refcursor(text, text, refcursor)

-- DROP FUNCTION IF EXISTS plpyodk.create_table_from_refcursor(text, text, refcursor);

CREATE OR REPLACE FUNCTION plpyodk.create_table_from_refcursor(
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
 RAISE INFO 'starting create_table_from_refcursor() for table %',_table_name; 
    FETCH FIRST FROM _ref INTO _row;
    SELECT _sql_val || '
           (' ||
           STRING_AGG(concat('"',val.key :: text,'" text', CASE WHEN val.key = 'data_id' THEN ' UNIQUE' ELSE NULL::text END), ',') ||
           ')'
        INTO _sql_val
    FROM JSON_EACH(TO_JSON(_row)) val;
  _sql = '
          CREATE TABLE IF NOT EXISTS ' || _schema_name ||'.'|| _table_name || '
          ' || _sql_val;
          
-- RAISE INFO 'SQL script for table cration %',_sql; 
    EXECUTE (_sql);
	
	/* adding new columns if table already exixts */
	SELECT _sql_new_cols || 
           STRING_AGG(concat('ALTER TABLE ' , _schema_name ,'.', _table_name , ' ADD COLUMN IF NOT EXISTS "',val.key :: text,'" text'), ';') ||';'
        INTO _sql_new_cols
    FROM JSON_EACH(TO_JSON(_row)) val
	WHERE val.key NOT IN ( SELECT attname 
 FROM pg_class JOIN pg_attribute ON pg_attribute.attrelid=pg_class.oid
 JOIN pg_namespace ON relnamespace = pg_namespace.oid
 WHERE nspname = _schema_name
   AND relkind = 'r' AND pg_class.relname = _table_name AND attnum > 0 AND attname = val.key
) AND plpyodk.does_table_exists(_schema_name, _table_name);
-- Create new attributes or Run a dummy query if nothing new
-- RAISE INFO 'SQL script for new cols %',_sql_new_cols; 
    EXECUTE (COALESCE(_sql_new_cols,'SELECT true;')); 
 RAISE INFO 'exiting from  create_table_from_refcursor() for table %',_table_name; 
-- RAISE INFO 'create_table_from_refcursor(): SQL statement is: %', COALESCE(_sql_new_cols,'no new column to add');
END;
$BODY$;

COMMENT ON FUNCTION plpyodk.create_table_from_refcursor(text, text, refcursor)
    IS 'description : 
	-> inspired by https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Create a table corresponding to the cursor structure (attribute types and names). As json atributes are not typed, all attributes are created as text ones.
	You''ll need to cast each in your subsequent requests.
	
	parameters :
	_table_name text 		-- the name of the table to create
	_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void';

-- FUNCTION: plpyodk.insert_into_from_refcursor(text, text, refcursor)

-- DROP FUNCTION IF EXISTS plpyodk.insert_into_from_refcursor(text, text, refcursor);

CREATE OR REPLACE FUNCTION plpyodk.insert_into_from_refcursor(
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

COMMENT ON FUNCTION plpyodk.insert_into_from_refcursor(text, text, refcursor)
    IS '	
	description :
	-> adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381
	Fills the table with data
	
	parameters :
		_schema_name text, 		-- the name of the schema where to create the table
		_table_name text, 		-- the name of the table to create
		_ref refcursor			-- the name of the refcursor to get data from
	
	returning :
	void';



-- FUNCTION: plpyodk.feed_data_tables_from_central(text, text, text)

-- DROP FUNCTION IF EXISTS plpyodk.feed_data_tables_from_central(text, text, text);

CREATE OR REPLACE FUNCTION plpyodk.feed_data_tables_from_central(
	schema_name text,
	form_id text,
	geojson_columns text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
--declare keys_to_ignore text;
declare 
	non_empty boolean;
	t record;
	query text;

BEGIN

query := 'select DISTINCT tablename	FROM '||schema_name||'.'||form_id;

	for t in execute query
	loop

	EXECUTE format('SELECT exists(select 1 FROM %1$s.%2$s WHERE tablename = ''%3$s'') ', schema_name, form_id, t.tablename)
	INTO non_empty;
		IF non_empty THEN 
		RAISE INFO 'entering feed_data_tables_from_central for table %', t.tablename; 
		EXECUTE format('DROP TABLE IF EXISTS data_table;
			CREATE TABLE data_table(tablename text, data_id text, key text, value json);
			INSERT INTO  data_table(tablename, data_id, key, value) 
			WITH RECURSIVE doc_key_and_value_recursive(tablename, data_id, key, value) AS (
			  SELECT tablename, 
				(json_data ->> ''__id'') AS data_id,
				t.key,
				t.value
			  FROM datas, json_each(json_data) AS t
			  UNION ALL
			  SELECT tablename, 
				doc_key_and_value_recursive.data_id,
				t.key,
				t.value
			  FROM doc_key_and_value_recursive,
				json_each(CASE 
				  WHEN json_typeof(doc_key_and_value_recursive.value) <> ''object'' 
						  OR key = ANY(string_to_array('''||geojson_columns||''','','')) 
						  THEN ''{}'' :: JSON
				  ELSE doc_key_and_value_recursive.value
				END) AS t
			), datas AS (
			SELECT tablename, json_data
		FROM '||schema_name||'.'||form_id||' WHERE tablename = '''||t.tablename||'''
					  ) SELECT tablename, data_id, key, value FROM doc_key_and_value_recursive WHERE json_typeof(value) <> ''object'' OR key = ANY(string_to_array('''||geojson_columns||''','','')) ORDER BY 2,1;'
		);

				EXECUTE format('SELECT plpyodk.dynamic_pivot(''SELECT data_id, key, value FROM data_table ORDER BY 1,2'',''SELECT DISTINCT key FROM data_table ORDER BY 1'',''curseur_central'');
									SELECT plpyodk.create_table_from_refcursor('''||schema_name||''','''||form_id||'_'||t.tablename||'_data'', ''curseur_central'');
									MOVE BACKWARD FROM "curseur_central";
									SELECT plpyodk.insert_into_from_refcursor('''||schema_name||''','''||form_id||'_'||t.tablename||'_data'', ''curseur_central'');
									CLOSE "curseur_central"'
							  );	
				RAISE INFO 'exiting from feed_data_tables_from_central for table %', t.tablename; 

		ELSE
			RAISE INFO 'table % is empty !', t.tablename; 
		END IF;
    end loop;
END;
$BODY$;

-- FUNCTION: plpyodk.does_table_exists(text, text)

-- DROP FUNCTION IF EXISTS plpyodk.does_table_exists(text, text);

CREATE OR REPLACE FUNCTION plpyodk.does_table_exists(
	schemaname text,
	tablename text)
    RETURNS boolean
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
		SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE  table_schema = $1
   AND    table_name   = $2
   );
	
$BODY$;

COMMENT ON FUNCTION plpyodk.does_table_exists(text, text)
    IS 'description : 
	checks if a table exists given its name and schema name
	
	parameters :
	schemaname text 		-- the name of the schema
	tablename text		-- the name of the table
	
	returning :
	boolean';


-- FUNCTION: plpyodk.odk_central_to_pg2(integer, text, text, text, text)

-- DROP FUNCTION IF EXISTS plpyodk.odk_central_to_pg2(integer, text, text, text, text);

CREATE OR REPLACE FUNCTION plpyodk.odk_central_to_pg2(
	project_id integer,
	form_id text,
	destination_schema_name text,
	criteria text,
	geojson_columns text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
EXECUTE format('DROP TABLE IF EXISTS '||destination_schema_name||'.'||form_id||';
	CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||form_id||' AS
	SELECT key as tablename, (json_array_elements(value)) as json_data
	FROM json_each(plpyodk.get_filtered_complete_submissions('''||project_id||'''::text, '''||form_id||'''::text,'''||criteria||'''::text)::json)
');

EXECUTE format('SELECT plpyodk.feed_data_tables_from_central('''||destination_schema_name||''', '''||form_id||''', '''||geojson_columns||''');'
);
END;
$BODY$;