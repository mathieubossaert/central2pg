-- FUNCTION: odk_central.insert_into_from_refcursor(text, refcursor)

-- DROP FUNCTION odk_central.insert_into_from_refcursor(text, refcursor);

CREATE OR REPLACE FUNCTION odk_central.insert_into_from_refcursor(
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
  _row       record;
  _hasvalues boolean = FALSE;
BEGIN

  LOOP   --for each row
    FETCH _ref INTO _row;
    EXIT WHEN NOT found;   --there are no rows more

    _hasvalues = TRUE;

    SELECT _sql_val || '
           (' ||
           STRING_AGG(
			   concat(
				   CASE WHEN val.value::text='null' OR val.value::text='' OR val.value::text='\null' OR val.value::text='"null"'
				   THEN 'null'
				   ELSE 
				   concat('''',replace(trim(val.value :: text,'\"'),'''',''''''),'''')
				   END)
			   , ',') ||
           '),'
        INTO _sql_val
    FROM JSON_EACH(TO_JSON(_row)) val;
  END LOOP;

  _sql_val = TRIM(TRAILING ',' FROM _sql_val);
  _sql = '
          INSERT INTO ' || _table_name || '
          VALUES ' || _sql_val ||' ON CONFLICT (data_id) DO NOTHING;';
  --RAISE NOTICE 'insert_into_from_refcursor(): SQL is: %', _sql;
  IF _hasvalues THEN    --to avoid error when trying to insert 0 values
    EXECUTE (_sql);
  END IF;
END;
$BODY$;

ALTER FUNCTION odk_central.insert_into_from_refcursor(text, refcursor)
    OWNER TO dba;
