/*
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
          
-- RAISE INFO 'SQL script for table cration %',_sql; 
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
-- RAISE INFO 'SQL script for new cols %',_sql_new_cols; 
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
	void';

