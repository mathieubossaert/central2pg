-- FUNCTION: odk_central.push_json_media_to_central(text, text, text, integer, text, text, text)

-- DROP FUNCTION IF EXISTS odk_central.push_json_media_to_central(text, text, text, integer, text, text, text);

CREATE OR REPLACE FUNCTION odk_central.push_json_media_to_central(
	email text,
	password text,
	central_domain text,
	project_id integer,
	form_id text,
	chemin_vers_media text,
	nom_media text)
    RETURNS void
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare url text;
declare requete text;
BEGIN
url = concat('https://',central_domain,'/v1/projects/',project_id,'/forms/',form_id,'/draft/attachments/',nom_media);
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' --header "Content-Type: application/geojson" --data-binary "@'||chemin_vers_media||'/'||nom_media||'" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION odk_central.push_json_media_to_central(text, text, text, integer, text, text, text)
    IS 'description :
	returning :';
