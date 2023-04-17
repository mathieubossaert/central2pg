

/*
FUNCTION: get_form_version(text, text, text, integer, text)
	description
		Asks central for the current version of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_form_version(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare current_version text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id);
EXECUTE (
		'DROP TABLE IF EXISTS form_version;
		 CREATE TEMP TABLE form_version(form_data json);'
		);
EXECUTE format('COPY form_version FROM PROGRAM $$ curl --insecure --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' '''||url||''' $$ ;');
SELECT form_data->>'version' INTO current_version FROM form_version;
RETURN current_version;
END;
$BODY$;


COMMENT ON FUNCTION odk_central.get_form_version(text, text, text, integer, text)
    IS '
	description
		Asks central for the current version of the given form. Returns it as a text.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		text';