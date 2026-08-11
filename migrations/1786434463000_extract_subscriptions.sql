-- Subscription management is now an optional, user-installed SQL package under
-- extensions/subscriptions. Existing installations with subscriptions must run
-- that package's upgrade migration before starting this pgstream version.
DO $$
DECLARE
    v_has_subscriptions boolean;
BEGIN
    IF to_regclass('pgstream.subscriptions') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM pgstream.subscriptions)'
        INTO v_has_subscriptions;

        IF v_has_subscriptions THEN
            RAISE EXCEPTION USING
                MESSAGE = 'legacy pgstream subscriptions must be upgraded before pgstream can start',
                DETAIL = 'The pgstream.subscriptions table still contains subscription definitions.',
                HINT = 'Run extensions/subscriptions/upgrades/from-pgstream-0.1.sql as the owner of the subscribed tables, then start pgstream again.';
        END IF;
    END IF;
END;
$$;

DO $$
BEGIN
    IF to_regclass('pgstream.subscriptions') IS NOT NULL THEN
        DROP TRIGGER IF EXISTS sync_database_trigger ON pgstream.subscriptions;
        DROP TABLE pgstream.subscriptions;
    END IF;
END;
$$;
DROP FUNCTION IF EXISTS pgstream.sync_database_trigger();
DROP FUNCTION IF EXISTS pgstream.build_extensions(jsonb);
DROP FUNCTION IF EXISTS pgstream.build_payload_from_extensions(jsonb);
DROP TYPE IF EXISTS pgstream.operation_type;
