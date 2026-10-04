{% snapshot snap_skills %}

{{
    config(
        unique_key    = 'SKILL_ID',
        strategy      = 'check',
        check_cols    = ['SKILL_NAME', 'SKILL_CATEGORY', 'IS_ACTIVE']
    )
}}

-- Snapshot the current state of each skill.
-- dbt will automatically detect changes to SKILL_NAME, SKILL_CATEGORY, or IS_ACTIVE
-- and create a new version row with dbt_valid_from / dbt_valid_to timestamps.

SELECT
    SKILL_ID,
    SKILL_NAME,
    SKILL_CATEGORY,
    IS_ACTIVE,
    SKILL_CODE
FROM {{ ref('silver_skills') }}

{% endsnapshot %}
