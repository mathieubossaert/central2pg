/*
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
EXECUTE format('COPY central_json_from_central FROM PROGRAM $$ curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET "'||url||'" -H "Accept: application/json" -H ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' $$ CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
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
		TABLE(user_name text, pass_word text, central_fqdn text, project integer, form text, tablename text)';

