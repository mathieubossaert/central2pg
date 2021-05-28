/*
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
	
-> is adapted from https://stackoverflow.com/questions/50837548/insert-into-fetch-all-from-cant-be-compiled/52889381#52889381';


