


/*
FUNCTION: push_media_to_central(text, text, text, integer, text, text, text)
	description
		Pushes the given file as an attachment of the current draft of the given form to Central.
		The function checks the file extension and adapt the content type header of the curl command.
		
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		media_path text					-- the path where the file can be find
		media_name text					-- the name of the file with its extension (xml or geojson or csv)
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION push_media_to_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	media_path text,
	media_name text -- must end by .xml or .geojson or .csv
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare content_type text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft/attachments/',media_name);
content_type = CASE reverse(split_part(reverse(media_name),'.',1)) -- to be sure to get string after last point in the filename (if other were used : toto.2022.xml)
	WHEN 'csv' THEN 'text.csv'
	WHEN 'geojson' THEN 'application/geojson'
	WHEN 'xml' THEN 'application/xml'
END;
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);
		 SET search_path=odk_central,public;'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --request POST --header ''Authorization: Bearer '||get_token_from_central(email, password, central_domain)||''' --header "Content-Type: '||content_type||'" --data-binary "@'||media_path||'/'||media_name||'" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION push_media_to_central(text, text, text, integer, text, text, text)
    IS 'description
		Pushes the given file as an attachment of the current draft of the given form to Central
		The function checks the file extension and adapt the content type header of the curl command.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		media_path text					-- the path where the file can be find
		media_name text					-- the name of the file with its extension (xml or geojson or csv)
	
	returning :
		void';