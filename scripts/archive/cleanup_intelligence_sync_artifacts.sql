\set ON_ERROR_STOP on
\if :{?apply}
\else
\set apply 0
\endif

\echo ''
\echo 'Intelligence sync cleanup'
\echo 'apply mode:' :apply
\echo ''

BEGIN;

CREATE TEMP TABLE cleanup_bad_jobber_mappings AS
SELECT
    id,
    canonical_id,
    entity_type,
    tool_specific_id,
    convert_from(decode(tool_specific_id, 'base64'), 'UTF8') AS raw_id
FROM cross_tool_mapping
WHERE tool_name = 'jobber'
  AND (
      (convert_from(decode(tool_specific_id, 'base64'), 'UTF8') LIKE '%/Client/%'
       AND canonical_id NOT LIKE 'SS-CLIENT-%')
   OR (convert_from(decode(tool_specific_id, 'base64'), 'UTF8') LIKE '%/Job/%'
       AND canonical_id NOT LIKE 'SS-JOB-%')
   OR (convert_from(decode(tool_specific_id, 'base64'), 'UTF8') LIKE '%/Quote/%'
       AND canonical_id NOT LIKE 'SS-RECUR-%')
  );

CREATE TEMP TABLE cleanup_duplicate_tasks AS
WITH task_flags AS (
    SELECT
        t.id,
        EXISTS (
            SELECT 1
            FROM cross_tool_mapping m
            WHERE m.canonical_id = t.id
              AND m.tool_name = 'asana'
        ) AS has_mapping,
        COUNT(*) OVER (
            PARTITION BY
                COALESCE(title, ''),
                COALESCE(description, ''),
                COALESCE(project_name, ''),
                COALESCE(assignee_employee_id, ''),
                COALESCE(due_date, ''),
                COALESCE(completed_date, ''),
                COALESCE(status, ''),
                COALESCE(priority, '')
        ) AS grp_count,
        MAX(
            CASE WHEN EXISTS (
                SELECT 1
                FROM cross_tool_mapping m
                WHERE m.canonical_id = t.id
                  AND m.tool_name = 'asana'
            ) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                COALESCE(title, ''),
                COALESCE(description, ''),
                COALESCE(project_name, ''),
                COALESCE(assignee_employee_id, ''),
                COALESCE(due_date, ''),
                COALESCE(completed_date, ''),
                COALESCE(status, ''),
                COALESCE(priority, '')
        ) AS grp_has_mapping
    FROM tasks t
)
SELECT id
FROM task_flags
WHERE grp_count > 1
  AND grp_has_mapping = 1
  AND has_mapping = FALSE;

CREATE TEMP TABLE cleanup_duplicate_calendar_events AS
WITH cal_flags AS (
    SELECT
        c.id,
        EXISTS (
            SELECT 1
            FROM cross_tool_mapping m
            WHERE m.canonical_id = c.id
              AND m.tool_name = 'google'
        ) AS has_mapping,
        COUNT(*) OVER (
            PARTITION BY
                COALESCE(title, ''),
                COALESCE(event_type, ''),
                COALESCE(start_datetime, ''),
                COALESCE(end_datetime, ''),
                COALESCE(attendees, ''),
                COALESCE(related_client_id, ''),
                COALESCE(notes, '')
        ) AS grp_count,
        MAX(
            CASE WHEN EXISTS (
                SELECT 1
                FROM cross_tool_mapping m
                WHERE m.canonical_id = c.id
                  AND m.tool_name = 'google'
            ) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                COALESCE(title, ''),
                COALESCE(event_type, ''),
                COALESCE(start_datetime, ''),
                COALESCE(end_datetime, ''),
                COALESCE(attendees, ''),
                COALESCE(related_client_id, ''),
                COALESCE(notes, '')
        ) AS grp_has_mapping
    FROM calendar_events c
)
SELECT id
FROM cal_flags
WHERE grp_count > 1
  AND grp_has_mapping = 1
  AND has_mapping = FALSE;

CREATE TEMP TABLE cleanup_duplicate_marketing_campaigns AS
WITH campaign_flags AS (
    SELECT
        c.id,
        EXISTS (
            SELECT 1
            FROM cross_tool_mapping m
            WHERE m.canonical_id = c.id
              AND m.tool_name = 'mailchimp'
        ) AS has_mapping,
        COUNT(*) OVER (
            PARTITION BY
                COALESCE(name, ''),
                COALESCE(platform, ''),
                COALESCE(campaign_type, ''),
                COALESCE(subject_line, ''),
                COALESCE(send_date, ''),
                COALESCE(recipient_count, 0),
                COALESCE(open_rate, 0),
                COALESCE(click_rate, 0),
                COALESCE(conversion_count, 0)
        ) AS grp_count,
        MAX(
            CASE WHEN EXISTS (
                SELECT 1
                FROM cross_tool_mapping m
                WHERE m.canonical_id = c.id
                  AND m.tool_name = 'mailchimp'
            ) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                COALESCE(name, ''),
                COALESCE(platform, ''),
                COALESCE(campaign_type, ''),
                COALESCE(subject_line, ''),
                COALESCE(send_date, ''),
                COALESCE(recipient_count, 0),
                COALESCE(open_rate, 0),
                COALESCE(click_rate, 0),
                COALESCE(conversion_count, 0)
        ) AS grp_has_mapping
    FROM marketing_campaigns c
)
SELECT id
FROM campaign_flags
WHERE grp_count > 1
  AND grp_has_mapping = 1
  AND has_mapping = FALSE;

CREATE TEMP TABLE cleanup_duplicate_commercial_proposals AS
WITH prop_flags AS (
    SELECT
        p.id,
        EXISTS (
            SELECT 1
            FROM cross_tool_mapping m
            WHERE m.canonical_id = p.id
              AND m.tool_name IN ('pipedrive', 'hubspot')
        ) AS has_mapping,
        COUNT(*) OVER (
            PARTITION BY
                COALESCE(lead_id, ''),
                COALESCE(client_id, ''),
                COALESCE(title, ''),
                COALESCE(square_footage, 0),
                COALESCE(service_scope, ''),
                COALESCE(price_per_visit, 0),
                COALESCE(frequency, ''),
                COALESCE(monthly_value, 0),
                COALESCE(status, ''),
                COALESCE(sent_date, ''),
                COALESCE(decision_date, ''),
                COALESCE(notes, '')
        ) AS grp_count,
        MAX(
            CASE WHEN EXISTS (
                SELECT 1
                FROM cross_tool_mapping m
                WHERE m.canonical_id = p.id
                  AND m.tool_name IN ('pipedrive', 'hubspot')
            ) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY
                COALESCE(lead_id, ''),
                COALESCE(client_id, ''),
                COALESCE(title, ''),
                COALESCE(square_footage, 0),
                COALESCE(service_scope, ''),
                COALESCE(price_per_visit, 0),
                COALESCE(frequency, ''),
                COALESCE(monthly_value, 0),
                COALESCE(status, ''),
                COALESCE(sent_date, ''),
                COALESCE(decision_date, ''),
                COALESCE(notes, '')
        ) AS grp_has_mapping
    FROM commercial_proposals p
)
SELECT id
FROM prop_flags
WHERE grp_count > 1
  AND grp_has_mapping = 1
  AND has_mapping = FALSE;

\echo 'Candidate counts'
SELECT 'bad_jobber_mappings' AS bucket, COUNT(*) AS cnt FROM cleanup_bad_jobber_mappings
UNION ALL
SELECT 'duplicate_tasks', COUNT(*) FROM cleanup_duplicate_tasks
UNION ALL
SELECT 'duplicate_calendar_events', COUNT(*) FROM cleanup_duplicate_calendar_events
UNION ALL
SELECT 'duplicate_marketing_campaigns', COUNT(*) FROM cleanup_duplicate_marketing_campaigns
UNION ALL
SELECT 'duplicate_commercial_proposals', COUNT(*) FROM cleanup_duplicate_commercial_proposals
ORDER BY 1;

\echo ''
\echo 'Sample bad Jobber mappings'
SELECT canonical_id, entity_type, raw_id
FROM cleanup_bad_jobber_mappings
ORDER BY canonical_id
LIMIT 20;

\echo ''
\echo 'Sample duplicate task ids'
SELECT id
FROM cleanup_duplicate_tasks
ORDER BY id
LIMIT 20;

\echo ''
\echo 'Sample duplicate proposal ids'
SELECT id
FROM cleanup_duplicate_commercial_proposals
ORDER BY id
LIMIT 20;

\if :apply
\echo ''
\echo 'Applying cleanup...'

WITH deleted AS (
    DELETE FROM cross_tool_mapping
    WHERE id IN (SELECT id FROM cleanup_bad_jobber_mappings)
    RETURNING 1
)
SELECT 'deleted_bad_jobber_mappings' AS bucket, COUNT(*) AS cnt
FROM deleted;

WITH deleted AS (
    DELETE FROM tasks
    WHERE id IN (SELECT id FROM cleanup_duplicate_tasks)
    RETURNING 1
)
SELECT 'deleted_duplicate_tasks' AS bucket, COUNT(*) AS cnt
FROM deleted;

WITH deleted AS (
    DELETE FROM calendar_events
    WHERE id IN (SELECT id FROM cleanup_duplicate_calendar_events)
    RETURNING 1
)
SELECT 'deleted_duplicate_calendar_events' AS bucket, COUNT(*) AS cnt
FROM deleted;

WITH deleted AS (
    DELETE FROM marketing_campaigns
    WHERE id IN (SELECT id FROM cleanup_duplicate_marketing_campaigns)
    RETURNING 1
)
SELECT 'deleted_duplicate_marketing_campaigns' AS bucket, COUNT(*) AS cnt
FROM deleted;

WITH deleted AS (
    DELETE FROM commercial_proposals
    WHERE id IN (SELECT id FROM cleanup_duplicate_commercial_proposals)
    RETURNING 1
)
SELECT 'deleted_duplicate_commercial_proposals' AS bucket, COUNT(*) AS cnt
FROM deleted;

COMMIT;
\echo 'Cleanup committed.'
\else
ROLLBACK;
\echo 'Dry run only. Transaction rolled back.'
\endif
