-- FUNCTION: odk_central.create_table_from_refcursor(text, refcursor)

-- DROP FUNCTION odk_central.create_table_from_refcursor(text, refcursor);

CREATE OR REPLACE FUNCTION odk_central.create_table_from_refcursor(
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
  _row       record;
BEGIN
    FETCH FIRST FROM _ref INTO _row;
    SELECT _sql_val || '
           (' ||
           STRING_AGG(concat('"',val.key :: text,'" text'), ',') ||
           ')'
        INTO _sql_val
    FROM JSON_EACH(TO_JSON(_row)) val;
  _sql = '
          CREATE TABLE IF NOT EXISTS ' || _table_name || '
          ' || _sql_val;
    EXECUTE (_sql);
  _sql_index = 'CREATE UNIQUE INDEX IF NOT EXISTS '||replace(_table_name,'.','_')||'_id_idx
    ON '||_table_name||' USING btree (data_id)
    TABLESPACE pg_default;';
    EXECUTE (_sql_index);
END;
$BODY$;
