CREATE OR REPLACE FUNCTION alter_role_if_exists(
    role_name text,
    utility_query text)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$alter_role_if_exists$$;

COMMENT ON FUNCTION alter_role_if_exists(
    role_name text,
    utility_query text)
    IS 'runs the utility query, if the role exists';
    