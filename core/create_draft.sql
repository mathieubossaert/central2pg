

/*
FUNCTION: create_draft(text, text, text, integer, text)
	description
		Creates a new draft of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.create_draft(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft?ignoreWarnings=true');
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl  --insecure --include --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' --header "Content-Type:" --data-binary "" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION odk_central.create_draft(text, text, text, integer, text)
    IS 'description :
		Creates a new draft of the given form.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central fqdn : central.mydomain.org
		project_id integer				-- the Id of the project ex. 4
		form_id text					-- the name of the Form ex. Sicen
	
	returning :
		void';