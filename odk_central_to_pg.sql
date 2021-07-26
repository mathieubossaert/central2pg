/*
FUNCTION: odk_central_to_pg(text, text, text, integer, text, text)
	description
		Retrieve all data from a given form to postgresql tables in the destination_schema.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.odk_central_to_pg(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	destination_schema_name text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
EXECUTE format('SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_fqdn,
	project,
	form,
	tablename,
	'''||destination_schema_name||''',
concat(''form_'',lower(form),''_'',lower(split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.''))))))
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');

EXECUTE format('
SELECT odk_central.feed_data_tables_from_central(
	'''||destination_schema_name||''',concat(''form_'',lower(form),''_'',lower(split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.''))))))
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||form_id||''');');
END;
$BODY$;

ALTER FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text)
    OWNER TO dba;

COMMENT ON FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text)
    IS 'description :
		wrap the calling of both functions get_submission_from_central() and feed_data_tables_from_central() functions 
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		form_table_name text			-- the table of the form to get value from (one of thoses returned by get_form_tables_list_from_central() function
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
	
	returning :
		void
';