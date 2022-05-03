-- FUNCTION: odk_central.publish_form_version(text, text, text, integer, text, integer)

-- DROP FUNCTION IF EXISTS odk_central.publish_form_version(text, text, text, integer, text, integer);

CREATE OR REPLACE FUNCTION odk_central.publish_form_version(
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
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --include --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' '''||url||''' $$ ;');
/*
curl --include --request POST --header 'Authorization: Bearer azertyuiopqsdfgh' 'https://myodk.server.fr/v1/projects/3/forms/select_from_geojson/draft/publish?version=2022040717'
*/
END;
$BODY$;


COMMENT ON FUNCTION odk_central.publish_form_version(text, text, text, integer, text, integer)
    IS 'description :
	returning :
	';
