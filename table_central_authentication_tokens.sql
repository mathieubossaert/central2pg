


/*
TABLE: central_authentication_tokens(text, text, text)

	description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens

*/

CREATE TABLE IF NOT EXISTS central_authentication_tokens
(
    url text COLLATE pg_catalog."default" NOT NULL,
    central_token text COLLATE pg_catalog."default",
    expiration timestamp with time zone,
    CONSTRAINT central_authentication_tokens_pkey PRIMARY KEY (url)
);

COMMENT ON TABLE  central_authentication_tokens 
	IS 'description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens';