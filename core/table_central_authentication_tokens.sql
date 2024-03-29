

/*
TABLE: central_authentication_tokens(text, text, text)

	description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		username text 						-- Central username
		password text 						-- Username's password
		project_id integer					-- project_id
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens

*/

CREATE TABLE IF NOT EXISTS odk_central.central_authentication_tokens
(
    url text COLLATE pg_catalog."default" NOT NULL,
    username text,
    password text,
    project_id integer,
    central_token text COLLATE pg_catalog."default",
    expiration timestamp with time zone,
    CONSTRAINT central_authentication_tokens_pkey PRIMARY KEY (url)
);

COMMENT ON TABLE  odk_central.central_authentication_tokens 
	IS 'description :
		Table to store Authentication token for several central servers
		
	attributes :
		url text							-- Central server FQDN
		username text 						-- Central username
		password text 						-- Username''s password
		project_id integer					-- project_id
		central_token text					-- The last token from this Central server
		expiration timestamp with time zone	-- valid until this timestamp
		
	comment :
		to be done : add user specific tokens';