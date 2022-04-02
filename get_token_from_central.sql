-- FUNCTION: odk_central.get_token_from_central(text, text, text)

-- DROP FUNCTION IF EXISTS odk_central.get_token_from_central(text, text, text);

CREATE OR REPLACE FUNCTION get_token_from_central(
	_email text,
	_password text,
	_central_domain text)
    RETURNS text
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
WITH tokens AS (SELECT url, central_token, expiration
	FROM odk_central.central_authentication_tokens
	WHERE url = _central_domain
	UNION 
	SELECT _central_domain,null,'1975-12-01'::timestamp with time zone),
more_recent_token AS (
SELECT url, central_token, expiration
FROM tokens ORDER BY expiration DESC LIMIT 1)
SELECT CASE 
	WHEN expiration >= now()::timestamp with time zone THEN central_token 
	ELSE (Select central_token FROM odk_central.get_fresh_token_from_central(_email, _password, _central_domain)) 
END as jeton 
	   FROM more_recent_token
$BODY$;
