


/*
FUNCTION: get_form_tables_list_from_central(text, text, text, integer, text, text, text, text, text)
	description :
		Download each media mentioned in submissions
	
	parameters :
		email text              -- the login (email adress) of a user who can get submissions
		password text           -- his password
		central_domain text     -- ODK Central fqdn : central.mydomain.org
		project_id integer      -- the Id of the project ex. 4
		form_id text            -- the name of the Form ex. Sicen
		submission_id text      -- the submission_id
		image text              -- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text        -- Where you want curl to store the file (path to directory)
		output text             -- filename with extension
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION get_file_from_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	submission_id text,
	image text,
	destination text,
	output text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
BEGIN
url = replace(concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/Submissions/',submission_id,'/attachments/',image),' ','%%20');
EXECUTE format('DROP TABLE IF EXISTS central_media_from_central;');
EXECUTE format('CREATE TEMP TABLE central_media_from_central(reponse text);');
EXECUTE format('SET search_path=odk_central,public; COPY central_media_from_central FROM PROGRAM $$ curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -X GET '||url||' -o '||destination||'/'||output||' -H "Accept: application/json" -H ''Authorization: Bearer '||get_token_from_central(email, password, central_domain)||''' $$ ;');
END;
$BODY$;

COMMENT ON FUNCTION get_file_from_central(text, text, text, integer, text, text, text, text, text)
    IS 'description :
		Download each media mentioned in submissions
	
	parameters :
		email text				-- the login (email adress) of a user who can get submissions
		password text			-- his password
		central_domain text 	-- ODK Central fqdn : central.mydomain.org
		project_id integer		-- the Id of the project ex. 4
		form_id text			-- the name of the Form ex. Sicen
		submission_id text		-- the submission_id
		image text				-- the image name mentionned in the submission ex. 1611941389030.jpg
		destination text		-- Where you want curl to store the file (path to directory)
		output text				-- filename with extension
	
	returning :
		void';