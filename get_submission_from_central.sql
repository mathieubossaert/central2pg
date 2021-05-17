-- FUNCTION: odk_central.get_submission_from_central(text, text, text, integer, text, text, text, text, text, text, text)
/* 
	futurer version should use filters... With more parameters

*/
-- DROP FUNCTION odk_central.get_submission_from_central(text, text, text, integer, text, text, text, text, text, text, text);

CREATE OR REPLACE FUNCTION odk_central.get_submission_from_central(
	email text,						-- the login (email adress) of a user who can get submissions
	password text,					-- his password
	central_domain text, 			-- ODK Central fqdn : central.mydomain.org
	project_id integer,				-- the Id of the project ex. 4
	form_id text,					-- the name of the Form ex. Sicen
	form_table_name text,			-- the table of the form to get value from (one of thoses returned by get_form_tables_list_from_central() function
	column_to_filter text,			-- the column (__system/submitterId or __system/submissionDate  on wich you want to apply a filter (only works on Submissions table
	filter text,					-- the filter to apply (gt = greater than, lt = lower than)
	filter_value text,				-- the value to compare the column with
	destination_schema_name text, 	-- the name of the schema where to create the permanent table 
	destination_table_name text		-- the name of this table 
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'.svc/',form_table_name,'?%%24filter=');
EXECUTE (
		'DROP TABLE IF EXISTS central_json_from_central;
		 CREATE TEMP TABLE central_json_from_central(form_data json);'
		);
EXECUTE format('COPY central_json_from_central FROM PROGRAM ''curl -k --connect-timeout 5 --max-time 10 --retry 5 --retry-delay 0 --retry-max-time 40 --user "'||email||':'||password||'" "'||url||column_to_filter||'%%20'||filter||'%%20'||filter_value||'"'' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
EXECUTE format('CREATE TABLE IF NOT EXISTS '||destination_schema_name||'.'||destination_table_name||' (form_data json);');
EXECUTE format ('CREATE UNIQUE INDEX IF NOT EXISTS '||destination_table_name||'_id_idx
    ON '||destination_schema_name||'.'||destination_table_name||' USING btree
    ((form_data ->> ''__id''::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;');
EXECUTE format('INSERT into '||destination_schema_name||'.'||destination_table_name||'(form_data) SELECT json_array_elements(form_data -> ''value'') AS form_data FROM central_json_from_central ON CONFLICT ((form_data ->> ''__id''::text)) DO NOTHING;');
RAISE NOTICE  '%',requete;
END;
