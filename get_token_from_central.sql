DROP FUNCTION IF EXISTS odk_central.get_token_from_central(text, text, text);

CREATE OR REPLACE FUNCTION odk_central.get_token_from_central(
	_email text,
	_password text,
	_central_domain text)
    RETURNS text 
    AS $BODY$
SELECT CASE 
	WHEN expiration >= now()::timestamp with time zone THEN central_token 
	ELSE (Select central_token FROM odk_central.get_fresh_token_from_central(_email, _password, _central_domain)) 
END as jeton 
	   FROM odk_central.central_authentication_tokens
WHERE url = _central_domain 
$BODY$ LANGUAGE SQL;
; 

--SELECT odk_central.get_token_from_central('sig@cen-occitanie.org', 'B9^3}kh5nTS>', 'central.sicen.fr')