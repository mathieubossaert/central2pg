-- FUNCTION: odk_central.create_draft(text, text, text, integer, text)

-- DROP FUNCTION IF EXISTS odk_central.create_draft(text, text, text, integer, text);

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
	returning :
	';

