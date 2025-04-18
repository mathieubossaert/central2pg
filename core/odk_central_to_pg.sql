

/*
FUNCTION: odk_central_to_pg(text, text, text, integer, text, text)

	description :
		Wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\
	
	returning :
		void
		
Table name in PostgreSQL must be less or equal to 63 chatracters. So you have to keep in mind that table names are a concat of 'form_' string, form_id and repeat name and be concise when naming repeats and form_id and avoid special chars, space or accent in form ids.

*/

CREATE OR REPLACE FUNCTION odk_central.odk_central_to_pg(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	destination_schema_name text,
	geojson_columns text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
EXECUTE format('SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_FQDN,
	project,
	replace(form,''%%'',''%%%%''),
	tablename,
	'''||destination_schema_name||''',
	lower(trim(regexp_replace(left(concat(''form_'',CASE WHEN length(form)>41 THEN left(form,10) ELSE form END,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_''))
	)
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||replace(form_id,'%','%%')||''');');

EXECUTE format('
SELECT odk_central.feed_data_tables_from_central(
	'''||destination_schema_name||''',lower(trim(regexp_replace(left(concat(''form_'',CASE WHEN length(form)>41 THEN left(form,10) ELSE form END,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_'')),'''||geojson_columns||''')
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||replace(form_id,'%','%%')||''');');
END;
$BODY$;

COMMENT ON FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text, text)
    IS 'description :
		wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\
	
	returning :
		void';
		


/*
FUNCTION: odk_central_to_pg(text, text, text, integer, text, text)

	description :
		Wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.odk_central_to_pg(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	destination_schema_name text,
	geojson_columns text,
	last_submission_timestamp text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN
EXECUTE format('SELECT odk_central.get_submission_from_central(
	user_name,
	pass_word,
	central_FQDN,
	project,
	form,
	tablename,
	'''||destination_schema_name||''',
	lower(trim(regexp_replace(left(concat(''form_'',CASE WHEN length(form)>41 THEN left(form,10) ELSE form END,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_''))
	,'''||last_submission_timestamp||'''
	)
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||replace(form_id,'%','%%')||''');');

EXECUTE format('
SELECT odk_central.feed_data_tables_from_central(
	'''||destination_schema_name||''',lower(trim(regexp_replace(left(concat(''form_'',CASE WHEN length(form)>41 THEN left(form,10) ELSE form END,''_'',split_part(tablename,''.'',cardinality(regexp_split_to_array(tablename,''\.'')))),58), ''[^a-zA-Z\d_]'', ''_'', ''g''),''_'')),'''||geojson_columns||''')
FROM odk_central.get_form_tables_list_from_central('''||email||''','''||password||''','''||central_domain||''','||project_id||','''||replace(form_id,'%','%%')||''');');
END;
$BODY$;

COMMENT ON FUNCTION odk_central.odk_central_to_pg(text, text, text, integer, text, text, text, text)
    IS 'description :
		wraps the calling of both get_submission_from_central() and feed_data_tables_from_central() functions 
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		destination_schema_name text 	-- the name of the schema where to create the permanent table 
		geojson_columns text 			-- geojson colmuns to ignore in recursion, comma delimited list like ''geopoint_widget_placementmap,point,ligne,polygone'', depending on your question names. /!\ Beware of spaces ! /!\ 
		last_submission_timestamp		-- the timstamp to use as a filter (sumissions with submission date greater or equal to this parameter) 
	
	returning :
		void';