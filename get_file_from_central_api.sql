/*
FUNCTION: get_file_from_central_api(text, text, text, integer, text, text, text, text, text)
	description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION get_file_from_central_api(
	email text,				
	password text,			
	central_domain text, 	
	project_id integer,		
	form_id text,			
	submission_id text,     
	image text,				
	destination text,		
	output text				
	)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/Submissions/',submission_id,'/attachments/',image);
EXECUTE format('DROP TABLE IF EXISTS central_media_from_central;');
EXECUTE format('CREATE TEMP TABLE central_media_from_central(reponse text);');
EXECUTE format('COPY central_media_from_central FROM PROGRAM ''curl --insecure --max-time 30 --user "'||email||':'||password||'" -o '||destination||'/'||output||' "'||url||'"'';');
END;
$BODY$;

COMMENT ON FUNCTION get_file_from_central_api(text, text, text, integer, text, text, text, text, text) IS 'description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void';