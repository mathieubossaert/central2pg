

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