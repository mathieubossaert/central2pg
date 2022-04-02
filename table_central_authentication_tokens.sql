-- Table: odk_central.central_authentication_tokens

-- DROP TABLE IF EXISTS odk_central.central_authentication_tokens;

CREATE TABLE IF NOT EXISTS odk_central.central_authentication_tokens
(
    url text COLLATE pg_catalog."default" NOT NULL,
    central_token text COLLATE pg_catalog."default",
    expiration timestamp with time zone,
    CONSTRAINT central_authentication_tokens_pkey PRIMARY KEY (url)
)
