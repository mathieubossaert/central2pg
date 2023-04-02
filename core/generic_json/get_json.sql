CREATE OR REPLACE FUNCTION odk_central.get_json(
	url text,
	destination_schema_name text,
	destination_table_name text,
	unique_column text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare requete text;
BEGIN
EXECUTE (
		'DROP TABLE IF EXISTS table_from_json;
		 CREATE TEMP TABLE table_from_json(form_data json);'
		);
EXECUTE format('COPY table_from_json FROM PROGRAM ''curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 "'||url||'"'' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
EXECUTE format('CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||destination_table_name||' (form_data json);');

EXECUTE format ('CREATE UNIQUE INDEX IF NOT EXISTS '||destination_table_name||'_id_idx
    ON '||destination_schema_name||'.'||destination_table_name||' USING btree
    ((form_data ->> '''||unique_column||'''::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;');

EXECUTE format('INSERT into '||destination_schema_name||'.'||destination_table_name||'(form_data) SELECT json_array_elements(form_data -> ''data'') AS form_data 
			   FROM table_from_json 
			   ON CONFLICT ((form_data ->> '''||unique_column||'''::text)) DO NOTHING
			   ;');
END;
$BODY$;