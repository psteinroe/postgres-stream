-- This array is the complete desired subscription set for stream 1. Existing
-- definitions omitted from the array are deleted, which removes their triggers.
SELECT pgstream_subscriptions.set_subscriptions(
    1,
    ARRAY[
        ROW(
            NULL::uuid,
            'user-created',
            1::bigint,
            'INSERT'::pgstream_subscriptions.operation_type,
            'public',
            'users',
            'new.email_verified = true',
            ARRAY['id', 'email', 'created_at']::text[],
            '{"topic":"users"}'::jsonb,
            '[]'::jsonb,
            '[]'::jsonb
        )::pgstream_subscriptions.subscriptions
    ]
);
