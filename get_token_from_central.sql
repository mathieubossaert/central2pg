


/*
FUNCTION: get_token_from_central(text, text, text)	

	description :
		Return a valid token, from the database id it exists and is still valid, or ask a new one (calling get_fresh_token_from_central(text,texttext) function) from ODK Central and then update the token table in the database.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central 
	
	returning :
		void
*/

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
	FROM central_authentication_tokens
	WHERE url = _central_domain
	UNION 
	SELECT _central_domain,null,'1975-12-01'::timestamp with time zone),
more_recent_token AS (
SELECT url, central_token, expiration
FROM tokens ORDER BY expiration DESC LIMIT 1)
SELECT CASE 
	WHEN expiration >= now()::timestamp with time zone THEN central_token 
	ELSE (Select central_token FROM get_fresh_token_from_central(_email, _password, _central_domain)) 
END as jeton 
	   FROM more_recent_token
$BODY$;

COMMENT ON FUNCTION  get_token_from_central(text,text,text)
	IS 'description :
		Return a valid token, from the database id it exists and is still valid, or ask a new one (calling get_fresh_token_from_central(text,texttext) function) from ODK Central and then update the token table in the database.
	
	parameters :
		email text						-- the login (email adress) of a user who can get submissions
		password text					-- his password
		central_domain text 			-- ODK Central 
	
	returning :
		void'
;