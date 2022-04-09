"""
Script uses python multiprocessing to efficiently pull newly engineered features from over
800 different across over 40 servers.

New features are engineered usign complex SQL.
SQL queries for all new features are stored in the query_info dictionary.
"""

import csv
import multiprocessing as mp
import time
import psycopg2
import glob
import pandas as pd


def connect_to_db(server_no, client):
    try:
        return psycopg2.connect(
            host = f"10.21.0.1{server_no}",
            database = client,
            user = "phppgadmin",
            password = "_REMOVED_",
            options = '-c statement_timeout=300000'
        )
    except:
        return None


def get_db_connection(server_no, client = 'postgres'):
    db_connection = connect_to_db(server_no, client)
    return db_connection


def get_list_of_dbs(server_no):
    print(f"get_list_of_dbs for server {server_no}.......")
    db_connection = get_db_connection(server_no)
    # print(f"dbconnection: {db_connection}")

    if db_connection is None:
        return []

    cursor = db_connection.cursor()
    sql = f"""
        SELECT d.datname
        FROM pg_catalog.pg_database d
        """

    try:
        cursor.execute(sql)
    except Exception as err:
        print("ERROR: ", err)
        return

    sql_results = cursor.fetchall()
    cursor.close()
    db_connection.close()

    # print(f"get_list_of_dbs - sqlresults: {sql_results}")
    list_of_dbs = []
    for row in sql_results:
        if 'sandbox' in row[0]:
            continue
        list_of_dbs.append(row[0])

    return list_of_dbs


def get_data(client_server_no_tuple):

    client = client_server_no_tuple[0]
    server_no = client_server_no_tuple[1]
    shared_dict = client_server_no_tuple[2]
    sql = client_server_no_tuple[3]
    folder_name = client_server_no_tuple[4]

    # print(f"DB {client}...............")

    db_connection = get_db_connection(server_no, client = client)

    if db_connection is None:
        print(f'ERROR: Unable to establish connection to DB for {client}.')
        return

    cursor = db_connection.cursor()

    # sql = "SELECT student_id FROM students LIMIT 1"
    try:
        cursor.execute(sql)
    except Exception as err:
        print(f"ERROR ({client}): ", err)
        shared_dict[client] = 0
        db_connection.close()
        return

    try:
        sql_results = cursor.fetchall()
    except Exception as err:
        print(f"ERROR: ({client})", err)
        shared_dict[client] = 0
        db_connection.close()
        return

    print(f"Got results for {client} ({len(sql_results)})")

    cursor.close()
    db_connection.close()

    rowArrays = []

    for row in sql_results:
        rowArray = [value for value in row]
        rowArray.insert(0, client)
        rowArrays.append(rowArray)

    with open(f'{folder_name}/{client}_{folder_name}.csv', 'a') as file:
        wtr = csv.writer(file, delimiter = ',', lineterminator = '\n')
        colnames = [desc[0] for desc in cursor.description]
        colnames.insert(0, 'client')
        wtr.writerow(colnames)
        for row in rowArrays:
            wtr.writerow(row)

    shared_dict[client] = len(rowArrays)
    # print(f"...............DB {client}")
    return


def main(sql, folder_name):
    mp.set_start_method("spawn")

    directory = f'/Users/franck/Library/Mobile Documents/com~apple~CloudDocs/MSDS/UW MSDS/DS785 Capstone Project/client_health_sql/new_features_client_health/{folder_name}/'
    extension = 'csv'
    all_filenames = [i.replace(directory, '') for i in glob.glob(f'{directory}*.csv')]

    clients_with_data = [x[:x.index('_')] for x in all_filenames]
    print(f"clients with data: {len(clients_with_data)}")

    list_of_servers = []

    # 29 has 0 clients, 30 has 0 clients, 31 has 0 clients
    for i in range(1, 29):
        list_of_servers.append(f"{i:02d}")

    # for i in range(70, 80):
    #     list_of_servers.append(f"{i:02d}")

    # list_of_servers = ['03']
    print(list_of_servers)

    client_list = []
    pending_queries = []

    manager = mp.Manager()
    query_pool = mp.Pool(processes = 16)
    shared_dict = manager.dict()


    for server_index, server in enumerate(list_of_servers):

        # server_data = []

        if int(server) < 0:
            continue

        print("====================================")
        print(f"Server: {server}")
        print("====================================")
        list_of_dbs = get_list_of_dbs(server)
        print(list_of_dbs)

        # my_q = Queue()
        server_array = []
        db_count = 0
        for db in list_of_dbs:

            if db in ['postgres']:
                continue
            if 'template' in db:
                continue
            if 'portal' in db:
                continue
            if 'demo' in db:
                continue
            if 'jasper' in db:
                continue
            if 'job_queue' in db:
                continue
            if 'client' in db:
                continue
            if 'sqlboss' in db:
                continue
            if 'instance' in db:
                continue
            if 'lightster' in db:
                continue
            if 'infinitecampus' in db:
                continue
            if 'specialist' in db:
                continue
            if 'skyward' in db:
                continue
            if 'importtest' in db:
                continue
            if 'datatraining' in db:
                continue
            if 'template' in db:
                continue
            if 'postgres' in db:
                continue
            if 'csm' in db:
                continue
            if 'trainingwheels' in db:
                continue
            if '_backup' in db:
                continue
            if '_iris' in db:
                continue
            if '_ise' in db:
                continue
            if '_testing' in db:
                continue
            if '_candidate' in db:
                continue
            if db[0] == '_':
                continue
            if 'mwtest' in db:
                continue
            if 'staging' in db:
                continue
            if '_old' in db:
                continue

            if db not in clients_with_data:
                client_list.append(db)
                db_count += 1
                pending_queries.append((db, server, shared_dict, sql, folder_name))

        print(f"ADDED ALL DBs FOR SERVER {server} ({db_count})")

    result = query_pool.map_async(get_data, pending_queries)

    update_count = 0
    print(f"dict: {len(shared_dict)}, client_list: {len(client_list)}")
    while len(shared_dict) < len(client_list):
        count_remaining = len(client_list) - len(shared_dict)
        print(f"{count_remaining} remaining...")
        update_count += 1
        if update_count % 4 == 0:
            list_remaining = list(set(client_list) - set(list(shared_dict.keys())))
            print(list_remaining)
        time.sleep(15)

    result.get()

    all_filenames = [i.replace(directory, '') for i in glob.glob(f'{directory}*.csv')]
    # combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f"{directory}{f}") for f in all_filenames])
    # export to csv
    combined_csv.to_csv(f"{folder_name}/_all_{folder_name}.csv", index = False, encoding = 'utf-8-sig')
    print("fin.")

# folder_names: dna_prebuilt_report_usage, dna_system_admin_tenure

query_info = {
    'reports': {
        'folder_name':'dna_prebuilt_report_usage',
        'sql':"""       
        WITH
            reporting_periods AS (
                SELECT
                    d::date AS month_start,
                    (d + '1 month'::interval - '1 day'::interval)::date month_end
                FROM GENERATE_SERIES(
                        (DATE_TRUNC('month', CURRENT_DATE)::date - INTERVAL '5 years')::date,
                        (DATE_TRUNC('month', CURRENT_DATE::date) + INTERVAL '1 month' - INTERVAL '1 day')::date,
                        '1 month'::interval
                    ) AS d
            ),
            
            ay_start_end AS (
                SELECT
                    academic_year,
                    MIN(start_date) AS start_date,
                    MAX(end_date) AS end_date
                FROM session_dates
                GROUP BY academic_year
            ),
        
            sf_an AS (
                SELECT
                    COALESCE(CASE
                                 WHEN EXISTS(SELECT definition_id
                                             FROM config.definitions
                                             WHERE key ILIKE 'salesforce.account_number')
                                     THEN (
                                     SELECT value::text
                                     FROM config.entries
                                     WHERE definition_id = (
                                         SELECT definition_id
                                         FROM config.definitions
                                         WHERE key ILIKE 'salesforce.account_number'
                                     )
                                 )
                                 END, 'none'::text) AS sf_an
            ),
        
            report_usage_by_month AS (
                SELECT
                    reporting_periods.month_start,
                    j.title,
                    j.jasper_prebuilt_id,
                    COUNT(l.accessed_at) AS times_accessed,
                    COUNT(DISTINCT l.user_id) count_of_distinct_users
        
                FROM (
                    SELECT *
                    FROM reporting_periods rp
                    JOIN UNNEST(ARRAY ['Assessment Matrix Report', 'Assessment Response Frequency', 'Skills Assessment Parent Letter', 'Site Assessment Overview', 'Teacher Assessment Overview', 'Assessment Student Overview', 'Assessment Site Peer Comparison', 'Assessment Teacher Peer Comparison', 'Multiple Assessment Summary Report']) AS title ON TRUE
                ) AS reporting_periods
        
                JOIN reports.jasper_prebuilts j ON j.title = reporting_periods.title
                LEFT JOIN logs.resource_access_logs l
                          ON l.remote_id = j.jasper_prebuilt_id
                              AND l.accessed_at BETWEEN reporting_periods.month_start AND reporting_periods.month_end
                              AND resource_type = 'report_jasper_prebuilt'
        
                GROUP BY
                    reporting_periods.month_start,
                    j.title,
                    j.jasper_prebuilt_id
        
            )
        
        
        SELECT
            REPLACE(REPLACE(sf_an, '}', ''), '{', '') AS sf_an,
            month_start,
            ay_start_end.academic_year,
            ru.jasper_prebuilt_id,
            ru.title,
            ru.times_accessed,
            ru.count_of_distinct_users
        
        FROM report_usage_by_month ru
        LEFT JOIN ay_start_end
             ON ru.month_start BETWEEN ay_start_end.start_date AND ay_start_end.end_date
        JOIN sf_an ON TRUE
        
        ORDER BY title, month_start 
        """
    },
    'system_admin': {
        'folder_name': 'dna_system_admin_tenure',
        'sql':"""
        WITH
            sf_an AS (
                SELECT
                    COALESCE(CASE
                                 WHEN EXISTS(SELECT definition_id
                                             FROM config.definitions
                                             WHERE key ILIKE 'salesforce.account_number')
                                     THEN (
                                     SELECT value::text
                                     FROM config.entries
                                     WHERE definition_id = (
                                         SELECT definition_id
                                         FROM config.definitions
                                         WHERE key ILIKE 'salesforce.account_number'
                                     )
                                 )
                                 END, 'none'::text) AS sf_an
            ),
        
            reporting_periods AS (
                SELECT
                    d::date AS month_start,
                    (d + '1 month'::interval - '1 day'::interval)::date month_end
                FROM GENERATE_SERIES(
                        (DATE_TRUNC('month', CURRENT_DATE)::date - INTERVAL '5 years')::date,
                        (DATE_TRUNC('month', CURRENT_DATE::date) + INTERVAL '1 month' - INTERVAL '1 day')::date,
                        '1 month'::interval
                    ) AS d
            ),
        
            sys_admins AS (
        
                /*
        
                System Admin is defined as:
                    A user who completes the yearly DnA rollover, as indicated by the user
                    creating or editing a term.
        
                - Could NOT user the "System Admin" role to identify System Admins b/c the System
                  Admin role is routinely given to users who are not truly system admins.
                - Could NOT even make the "System Admin" role a pre-requisite of being a System Admin
                  b/c when System Admins leave the district, their site/role affiliations are
                  sometimes retroactively removed (i.e. mstuder at winters)
                - If a user identified as System Admin does not login for 3 months, they are no longer
                  considered a System Admin.
                - The "calculated" System Admin status goes back 3 months from the date a term was
                  created/edited b/c new System Admins sometimes start the role in mid-year, and
                  therefore do not have an opportunity to do a rollover right away.
                - This algorithm is definitely not perfect, but DOES seem to identify TRUE system admin
                  turnover relatively well.
        
                 */
        
                SELECT
                    rp.month_start, rp.month_end,
                    u.user_id, u.username,
                    MIN(log_time - INTERVAL '3 Months') AS rollover_date,
                    ARRAY_AGG(DISTINCT role_name) AS roles
                FROM logs.basic
                JOIN users u USING (user_id)
                RIGHT JOIN reporting_periods rp ON (log_time - INTERVAL '3 Months') <= rp.month_end
                LEFT JOIN user_term_role_aff utra ON u.user_id = utra.user_id
                LEFT JOIN roles USING (role_id)
                WHERE description ILIKE '%Updated term%'
                  AND u.username NOT ILIKE '%illuminator%'
                  AND (role_name IS NULL OR role_name != 'Illuminate Admin')
        
                GROUP BY rp.month_start, rp.month_end, u.user_id, u.username
                ORDER BY u.user_id, month_start
            ),
        
            sa_logins_last_3months AS (
        
                WITH
                    sa_monthly_login_count AS (
                        SELECT
                            rp2.month_start,
                            rp2.month_end,
                            sa.user_id,
                            COUNT(l.log_time) AS month_login_count,
                            MIN(sa.rollover_date) AS rollover_date
        
                        FROM reporting_periods rp2
                        JOIN sys_admins sa
                             ON sa.month_start = rp2.month_start
                        LEFT JOIN logs.basic l
                                  ON l.log_time BETWEEN rp2.month_start AND rp2.month_end
                                      AND l.user_id = sa.user_id
                                      AND description = 'User logged in'
        
        -- 		  AND l.log_time BETWEEN '2021-06-01'::date AND '2021-09-01'::date
        -- 		  AND l.log_time BETWEEN (CURRENT_DATE - INTERVAL '3 months') AND CURRENT_DATE
        
                        GROUP BY rp2.month_start, rp2.month_end, sa.user_id
                    ),
        
                    sa_monthly_login_count_with_last_3_months AS (
                        SELECT
                            sa_monthly_login_count.*,
                            u.username,
                            SUM(month_login_count)
                            OVER (PARTITION BY user_id ORDER BY user_id, month_start ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS login_count_last_3months -- previous 3 months login count
                        FROM sa_monthly_login_count
                        JOIN users u USING (user_id)
                    )
        
                    ----------------------
                    -- TROUBLE-SHOOTING --
                    ----------------------
        -- 		SELECT * FROM sa_monthly_login_count_with_last_4_months
        -- 		SELECT * FROM sa_monthly_login_count ORDER BY user_id, month_start
                    --
        
                SELECT
                    rp.month_start,
                    rp.month_end,
                    ARRAY_AGG(DISTINCT sa_logs.username) AS usernames,
                    MAX(month_login_count) AS month_login_count,
                    MAX(login_count_last_3months) AS login_count_last_3_months,
                    MIN(rollover_date) AS rollover_date
                FROM reporting_periods rp
                LEFT JOIN sa_monthly_login_count_with_last_3_months sa_logs
                          ON sa_logs.month_start = rp.month_start
                              AND sa_logs.login_count_last_3months > 1
        
                GROUP BY rp.month_start, rp.month_end
        
                ORDER BY rp.month_start
        
            )
        
        ----------------------
        -- TROUBLE-SHOOTING --
        ----------------------
        
        -- SELECT u.username, sys_admins.* FROM sys_admins JOIN users u USING(user_id);
        -- SELECT * FROM first_rollover;
        -- SELECT * FROM sa_logins_last_3months;
        
        ----------------------
        
        SELECT
            REPLACE(REPLACE(sf_an, '}', ''), '{', '') AS sf_an,
            l.*,
            COALESCE(EXTRACT(DAY FROM month_end - l.rollover_date), 0) AS sa_tenure_in_days
        FROM sa_logins_last_3months l
        JOIN sf_an ON TRUE  
"""
    },
    'common_asmts': {
        'folder_name': 'dna_common_assessments',
        'sql': """
        WITH
            sf_an AS (
                SELECT
                    COALESCE(CASE
                                 WHEN EXISTS(SELECT definition_id
                                             FROM config.definitions
                                             WHERE key ILIKE 'salesforce.account_number')
                                     THEN (
                                     SELECT value::text
                                     FROM config.entries
                                     WHERE definition_id = (
                                         SELECT definition_id
                                         FROM config.definitions
                                         WHERE key ILIKE 'salesforce.account_number'
                                     )
                                 )
                                 END, 'none'::text) AS sf_an
            ),
        
            ay_start_end AS (
                SELECT
                    academic_year,
                    MIN(start_date) AS start_date,
                    MAX(end_date) AS end_date
                FROM session_dates
                GROUP BY academic_year
            ),
        
            MONTHS AS (
                SELECT
                        DATE_TRUNC('month', CURRENT_DATE) - (INTERVAL '1 MONTH' * GENERATE_SERIES(0, 48)) AS month_start,
                        DATE_TRUNC('month', CURRENT_DATE) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' -
                        (INTERVAL '1 MONTH' * GENERATE_SERIES(0, 48)) AS month_end
            ),
        
            stu_count_per_assessment_per_site AS (
                SELECT
                    month_start,
                    se.academic_year,
                    asr.assessment_id,
                    a.title,
                    se.site_id,
                    COUNT(asr.student_id) AS stu_count_per_site
        
                FROM dna_assessments.agg_student_responses asr
                JOIN student_session_aff ssa
                     ON asr.student_id = ssa.student_id
                         AND asr.date_taken BETWEEN ssa.entry_date AND ssa.leave_date
                JOIN sessions se USING (session_id)
                JOIN dna_assessments.assessments a USING (assessment_id)
                JOIN MONTHS ON asr.date_taken BETWEEN MONTHS.month_start AND MONTHS.month_end
                GROUP BY se.academic_year, month_start, asr.assessment_id, a.title, se.site_id
        
                ORDER BY month_start DESC, a.title, stu_count_per_site DESC
            ),
        
            site_count_per_assessment AS (
                SELECT
                    month_start,
                    academic_year,
                    assessment_id,
                    title,
                    COUNT(site_id) AS site_count_per_assessment
                FROM stu_count_per_assessment_per_site scpaps
                WHERE stu_count_per_site > 10
                GROUP BY scpaps.academic_year, month_start, scpaps.assessment_id, scpaps.title
                HAVING COUNT(site_id) > 1
                ORDER BY month_start DESC, site_count_per_assessment DESC, title
            ),
        
            common_assessments_per_month AS (
                SELECT
                    academic_year,
                    month_start,
                    COUNT(assessment_id) AS common_assessment_count,
                    SUM(COUNT(assessment_id)) OVER (
                        PARTITION BY academic_year
                        ORDER BY month_start ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                        ) AS cumulative_common_assessment_count_per_ay
        
                FROM site_count_per_assessment
                GROUP BY academic_year, month_start
                ORDER BY month_start DESC
            )
        
        SELECT
            sf_an,
            month_start,
            ay_start_end.academic_year,
            COALESCE(ca.common_assessment_count, 0) AS common_assessment_count,
            COALESCE(ca.cumulative_common_assessment_count_per_ay, 0) AS cumulative_common_assessment_count_per_ay
        
        FROM months
        JOIN ay_start_end ON months.month_start BETWEEN ay_start_end.start_date AND ay_start_end.end_date
        LEFT JOIN common_assessments_per_month ca USING (month_start)
        JOIN sf_an ON TRUE
        ;
        """
    }
}


# Change 'reports' to 'system_admin' or 'common_asmts' to generate data for different features
if __name__ == '__main__':
    main(query_info['reports']['sql'], query_info['reports']['folder_name'])


