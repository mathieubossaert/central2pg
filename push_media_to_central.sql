-- FUNCTION: odk_central.push_media_to_central(text, text, text, integer, text, text, text)

-- DROP FUNCTION IF EXISTS odk_central.push_media_to_central(text, text, text, integer, text, text, text);

CREATE OR REPLACE FUNCTION odk_central.push_media_to_central(
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
content_type = CASE reverse(split_part(reverse('abc.tt.xml'),'.',1)) -- to be sure to get string after last point in the filename (if other were used : toto.2022.xml)
	WHEN 'csv' THEN 'text.csv'
	WHEN 'geojson' THEN 'application/geojson'
	WHEN 'xml' THEN 'application/xml'
END;
EXECUTE (
		'DROP TABLE IF EXISTS media_to_central;
		 CREATE TEMP TABLE media_to_central(form_data text);'
		);
EXECUTE format('COPY media_to_central FROM PROGRAM $$ curl --insecure --request POST --header ''Authorization: Bearer '||odk_central.get_token_from_central(email, password, central_domain)||''' --header "Content-Type: '||content_type||'" --data-binary "@'||media_path||'/'||media_name||'" '''||url||''' $$ ;');

END;
$BODY$;

COMMENT ON FUNCTION odk_central.push_media_to_central(text, text, text, integer, text, text, text)
    IS 'description :
	returning :';
