

/*
FUNCTION: get_fresh_token_from_central(text, text, text)

	description :
		Ask central for a new fresh token for the given Central server with given login and password. And update the database token table with it.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN
	
	returning :
		void
*/

CREATE OR REPLACE FUNCTION odk_central.get_fresh_token_from_central(
	email text,
	password text,
	central_domain text)
    RETURNS TABLE(url text, central_token text, expiration timestamp with time zone) 
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
    ROWS 1000

AS $BODY$
declare url text;
declare requete text;
BEGIN
requete = concat('curl --insecure --max-time 30 --retry 5 --retry-delay 0 --retry-max-time 40 -H "Content-Type: application/json" -H "Accept: application/json" -X POST -d ''''{"email":"',email,'","password":"',replace(password,'\','\\'),'"}'''' https://',central_domain,'/v1/sessions');

RAISE INFO 'requete curl : %',requete;

EXECUTE (
		'DROP TABLE IF EXISTS central_token;
		 CREATE TEMP TABLE central_token(form_data json);'
		);

EXECUTE format('COPY central_token FROM PROGRAM '''||requete||''' CSV QUOTE E''\x01'' DELIMITER E''\x02'';');
RETURN QUERY EXECUTE 
FORMAT('INSERT INTO odk_central.central_authentication_tokens(url, central_token, expiration)
	   SELECT '''||central_domain||''' as url, form_data->>''token'' as central_token, (form_data->>''expiresAt'')::timestamp with time zone as expiration FROM central_token 
	   ON CONFLICT(url) DO UPDATE SET central_token = EXCLUDED.central_token, expiration = EXCLUDED.expiration
	   RETURNING  url, central_token, expiration;');
END;
$BODY$;

COMMENT ON FUNCTION  odk_central.get_fresh_token_from_central(text,text,text)
	IS 'description :
		Ask central for a new fresh token for the given Central server with given login and password. And update the database token table with it.
		
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central FQDN
	
	returning :
		void';