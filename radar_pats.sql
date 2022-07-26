SELECT
    DISTINCT gp.patient_id AS "id",
    pnhs.number AS nhs_no,
    pchi.number AS chi_no,
    phsc.number AS hsc_no,
    pd.first_name AS first_name,
    pd.last_name AS surname,
    pd.date_of_birth AS birth_date,
    pd.date_of_death AS death_date,
    pd.gender AS sex,
    pa.postcode,
    hosp.name AS hospital
FROM
    (
        SELECT
            patient_id
        FROM
            group_patients
        WHERE
            group_id IN (123, 141)
    ) gp
    LEFT JOIN (
        SELECT
            patient_id,
            "number"
        FROM
            patient_numbers
        WHERE
            source_type = 'RADAR'
            AND number_group_id = 120
    ) pnhs ON gp.patient_id = pnhs.patient_id
    LEFT JOIN (
        SELECT
            patient_id,
            "number"
        FROM
            patient_numbers
        WHERE
            source_type = 'RADAR'
            AND number_group_id = 121
    ) pchi ON gp.patient_id = pchi.patient_id
    LEFT JOIN (
        SELECT
            patient_id,
            "number"
        FROM
            patient_numbers
        WHERE
            source_type = 'RADAR'
            AND number_group_id = 122
    ) phsc ON gp.patient_id = phsc.patient_id
    LEFT JOIN (
        SELECT
            patient_id,
            first_name,
            last_name,
            date_of_birth,
            date_of_death,
            gender
        FROM
            patient_demographics
        WHERE
            source_type = 'RADAR'
    ) pd ON gp.patient_id = pd.patient_id
    LEFT JOIN (
        SELECT
            pa1.patient_id,
            pa1.postcode
        FROM
            patient_addresses pa1
            INNER JOIN (
                SELECT
                    patient_id,
                    max(modified_date) as m_date
                FROM
                    patient_addresses
                WHERE
                    source_type = 'RADAR'
                GROUP BY
                    patient_id
            ) pa2 ON pa1.patient_id = pa2.patient_id
            AND pa1.modified_date = pa2.m_date
        WHERE
            source_type = 'RADAR'
    ) pa ON gp.patient_id = pa.patient_id
    LEFT JOIN patients ps ON gp.patient_id = ps.id
    LEFT JOIN (
        SELECT
            DISTINCT ON (patient_numbers.patient_id) patient_numbers.patient_id AS pat_id,
            groups.name
        FROM
            patient_numbers
            INNER JOIN groups ON patient_numbers.number_group_id = groups.id
        WHERE
            groups.type = 'HOSPITAL'
        ORDER BY
            patient_numbers.patient_id,
            patient_numbers.created_date DESC
    ) hosp ON gp.patient_id = hosp.pat_id
WHERE
    ps.test = false
ORDER BY
    id;