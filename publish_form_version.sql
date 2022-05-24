


/*
FUNCTION: publish_form_version(text, text, text, integer, text, integer)

	description
		Publishes the current draft of the given form with the given version number.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		version_number integer			-- the new version number to use
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION publish_form_version(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	version_number integer)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft/publish?version=',version_number);
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);
		 SET search_path=odk_central,public;'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --include --request POST --header ''Authorization: Bearer '||get_token_from_central(email, password, central_domain)||''' '''||url||''' $$ ;');

END;
$BODY$;


COMMENT ON FUNCTION publish_form_version(text, text, text, integer, text, integer)
    IS '
	description
		Publishes the current draft of the given form with the given version number.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
		version_number integer			-- the new version number to use
	
	returning :
		void';