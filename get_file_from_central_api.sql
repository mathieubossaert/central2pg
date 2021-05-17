-- FUNCTION: odk_central.get_file_from_central_api(text, text, text, text, text, text, text)

-- DROP FUNCTION odk_central.get_file_from_central_api(text, text, text, text, text, text, text);

CREATE OR REPLACE FUNCTION odk_central.get_file_from_central_api(
	email text,				-- the login (email adress) of a user who can get submissions
	password text,			-- his password
	central_domain text, 	-- ODK Central fqdn : central.mydomain.org
	project_id integer,		-- the Id of the project ex. 4
	form_id text,			-- the name of the Form ex. Sicen
	submission_id text,
	image text,				-- the image name mentionned in the submission ex. 1611941389030.jpg
	destination text,		-- Where you want curl to store the file (path to directory)
	output text				-- filename with extension
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
EXECUTE format('COPY central_media_from_central FROM PROGRAM ''curl -k --user "'||email||':'||password||'" -o '||destination||'/'||output||' "'||url||'"'';');
--curl --user "email:password" -o /home/postgres/medias_odk/fdsfdsu_1611941389030.jpg "https://central.mydomain.org/v1/projects/3/forms/Sicen/Submissions/uuid:5de3ee7b-8f3b-4b80-9dcb-e4cbc1ec7239/attachments/1611941389030.jpg"

END;
$BODY$;
