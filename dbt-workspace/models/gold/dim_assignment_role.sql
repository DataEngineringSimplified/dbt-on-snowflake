{{
    config(
        materialized = 'table'
    )
}}

SELECT DISTINCT
    {{ hash_key(['ASSIGNMENT_ROLE']) }} AS ASSIGNMENT_ROLE_HK,
    ASSIGNMENT_ROLE
FROM {{ ref('silver_employee_project_assignments') }}
WHERE ASSIGNMENT_ROLE IS NOT NULL
