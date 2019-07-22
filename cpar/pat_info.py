import pandas as pd
from CHECK.dbconnect import dbconnect
from CHECK.dbconnect import import_helpers


class DiagnosisCategorizer(object):

    def __init__(self, database, release_date, release_num):

        #qualifying ratio i,e, 3 inclusion codes for every 1 exclusion code to be diagnosed
        self.dx_ratio = {'SCD': .75}

        self.connection = dbconnect.DatabaseConnect(database)
        self.release_num = release_num

        self.diagnosis_tables = ['pat_info_dx_mental_health',
                                 'pat_info_dx_pregnancy',
                                 'pat_info_dx_primary']


    def pat_info_query(self):
        pat_info_sql = '''SELECT RecipientID, Program_Age, Gender,
                          Program_Date from pat_info_demo'''
        return self.connection.query(pat_info_sql, output_format='df')

    def dx_code_query(self):
        dx_code_query = '''SELECT RecipientID, DiagCd, count(*) ICD_Count
        from stage_diagnosis dx
        group by DiagCd, ICDVersion, RecipientID;'''
        return self.connection.query(dx_code_query, output_format='df')

    def diagnosis_category(self, df):
        '''Categorizes primary diagnosis into a single diagnosis column'''
        if df['ICD_List'] == '0':
            return "NA"
        if df['SCD'] == 1:
            return "SCD"
        if df['Prematurity'] > 0:
            return "Prematurity"
        if df['Epilepsy'] > 0:
            return "Neurological"
        if df['Brain_Injury'] > 0:
            return "Neurological"
        if df['Diabetes'] > 0:
            return "Diabetes"
        if df['Asthma'] > 0:
            return "Asthma"
        else:
            return "Other"

    def inclusion_exclusion_diagnoser(self, pt_dx_codes, dx_inc_exc_table, inc_exc_ratio=1, min_inc_count=1):
        '''
        pt_dx_codes: pd.DataFrame contains counts of all ICD diagnosis that the patient has recorded
        dx_inc_exc_table: pd.DataFrame that contains the inclusion and exclusion codes for a single diagnosis
        inc_exc_ratio: inclusion to exclusion ratio necessary to be diagnosed
        returns a list of patients that met the inclusion exclusion criteria for a diagnosis
        '''

        pt_dx_codes_merge = pd.merge(pt_dx_codes, dx_inc_exc_table,
                                     how='inner', on='DiagCd')

        inc_exc_rids = pd.pivot_table(pt_dx_codes_merge, index='RecipientID',
                                      columns='Incl_Excl', values='ICD_Count',
                                      aggfunc='sum', fill_value=0)

        if 'I' not in inc_exc_rids.columns:
            inc_exc_rids['I'] = 0
        if 'E' not in inc_exc_rids.columns:
            inc_exc_rids['E'] = 0
        inc_exc_rids['Inc_Exc_Ratio'] = inc_exc_rids['I'] / (inc_exc_rids['E'] + inc_exc_rids['I'])
        rid_list = inc_exc_rids.loc[(inc_exc_rids['Inc_Exc_Ratio']>=inc_exc_ratio)&
                                    (inc_exc_rids['I']>=min_inc_count)].index

        return rid_list

    def dx_table_iterator(self, pt_info, pt_dx_codes, inc_exc_table):
        '''for a diagnosis family (mh, pregnancy, primary) iterates through all diagnosis
        groups and adds column for each dx subgroup 1 being inclusion 0 being no diagnosis'''

        pt_info_cp = pt_info.copy()

        dx_list = inc_exc_table['Group_Name'].unique()

        for dx in dx_list:
            dx_inc_exc_table = inc_exc_table.loc[inc_exc_table['Group_Name']==dx]
            if dx in self.dx_ratio:
                ratio = self.dx_ratio[dx]
            else:
                ratio = 1

            rid_list = self.inclusion_exclusion_diagnoser(pt_dx_codes, dx_inc_exc_table, ratio)
            pt_info_cp.loc[pt_info_cp['RecipientID'].isin(rid_list), dx] = 1
            pt_info_cp[dx].fillna(0, inplace=True)

        pt_info_cp[dx_list] = pt_info_cp[dx_list].astype(int)
        return pt_info_cp

    def primary_dx(self, pt_dx_codes, pt_info):
        '''Prematurity must be less age 3 at Enrollment to be considered Premature'''
        inc_exc_table = self.connection.query( "SELECT * FROM dx_code_inc_exc_primary_diagnosis;", output_format='df')
        unique_rids = pt_dx_codes['RecipientID'].unique()
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)
        pt_dx_table.loc[pt_dx_table['Program_Age'] > 3, 'Prematurity'] = 0
        pt_dx_table.loc[:, 'ICD_List'] = '0'
        pt_dx_table.loc[pt_dx_table['RecipientID'].isin(unique_rids), 'ICD_List'] = '1'
        pt_dx_table['Diagnosis_Category'] = pt_dx_table.apply(self.diagnosis_category, axis=1)
        pt_dx_table['ReleaseNum'] = self.release_num
        return pt_dx_table

    def mh_dx(self, pt_dx_codes, pt_info):
        inc_exc_table = self.connection.query( "SELECT * FROM dx_code_inc_exc_mental_health;", output_format='df')
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)
        pt_dx_table['ReleaseNum'] = self.release_num
        return pt_dx_table

    def preg_dx(self, pt_dx_codes, pt_info):
        '''Determines if pregnancy icd code was ever given to patient
        Should only occur for Females over age 10 at time of enrollment'''

        inc_exc_table = self.connection.query( "SELECT * FROM dx_code_inc_exc_pregnancy;", output_format='df')
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)

        pt_dx_table.loc[(pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) > 0) &
                        (pt_dx_table['Program_Age']>10) & (pt_dx_table['Gender']=='Female'), 'Preg_Flag'] = 1
        pt_dx_table.loc[pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) == 0, 'Preg_Flag'] = 0

        pt_dx_table.loc[:, 'Preg_Flag'] = pt_dx_table['Preg_Flag'].fillna(0).astype(int)
        pt_dx_table['ReleaseNum'] = self.release_num
        return pt_dx_table

    def dx_inserter(self, dx_df, dx_table_name):

        self.connection.query('truncate {};'.format(dx_table_name))

        dx_table_cols = import_helpers.get_tbl_columns_query(self.connection,
                                                             dx_table_name)

        shared_cols = import_helpers.get_shared_columns(dx_table_cols,
                                                        dx_df.columns)

        insert_sql_str = import_helpers.insert_sql_generator(shared_cols,
                                                             import_tbl=dx_table_name)

        self.connection.insert(insert_sql_str, dx_df[shared_cols])

        return 'Inserted {}'.format(dx_table_name)


    def dx_wrapper(self, insert=False):
        '''Calculates for all of the diagnosis tables and if to_sql is True loads them into the
        database'''
        dx_dfs = {}
        pat_info_df = self.pat_info_query()
        dx_codes = self.dx_code_query()

        for table in self.diagnosis_tables:
            if table == 'pat_info_dx_primary':
                pat_dx_table = self.primary_dx(dx_codes, pat_info_df)
            elif table == 'pat_info_dx_pregnancy':
                pat_dx_table = self.preg_dx(dx_codes, pat_info_df)
            elif table == 'pat_info_dx_mental_health':
                pat_dx_table = self.mh_dx(dx_codes, pat_info_df)

            dx_dfs[table] = pat_dx_table

        if insert == False:
            return dx_dfs
        else:
            for table in dx_dfs.keys():
                print(self.dx_inserter(dx_dfs[table], table))
            return 'Inserted'



class RiskCategorizer(object):

    def __init__(self, database, release_date, release_num):

        self.release_date = release_date
        self.release_num = release_num
        self.connection = dbconnect.DatabaseConnect(database)

        self.risk_date_columns = {'Release_Date':'Current_Risk',
                                  'Engagement_Date':'Engagement_Risk',
                                  'Enrollment_Date':'Enrollment_Risk',
                                  'Randomization_Date':'Randomization_Risk',
                                  'Program_Date':'Program_Risk'}

    def pat_info_query(self):
        sql_str = '''SELECT RecipientID, Initial_Enrollment_Date Enrollment_Date,
        Engagement_Date, Randomization_Date,
        Program_Date
        FROM CHECK_CPAR2.pat_info_demo;'''
        pat_info_df = self.connection.query(sql_str, output_format='df')

        pat_info_df['Release_Date'] = self.release_date

        for i in self.risk_date_columns.keys():
            pat_info_df.loc[:, i] = pd.to_datetime(pat_info_df[i])

        return pat_info_df

    def ip_ed_query(self):
        sql_str = '''SELECT
            RecipientID,
            ServiceFromDt,
            ServiceThruDt,
            IF(Category1='Outpatient', 'ED', 'IP') Category,
            Encounter
        FROM
            stage_main_claims
        WHERE
            Category1 = 'INPATIENT'
                OR (Category1 = 'OUTPATIENT'
                AND CHECK_Category = 'ED')'''
        ip_ed_df = self.connection.query(sql_str, output_format='df')

        #IP takes precedent over ED on same day
        ip_ed_df['Category'] = pd.Categorical(ip_ed_df['Category'],
                                              categories=["IP", "ED"], ordered=True)
        ip_ed_df = ip_ed_df.sort_values(['RecipientID','ServiceFromDt','Category'])
        #converted back for speed
        ip_ed_df['Category'] = ip_ed_df['Category'].astype('str')

        ip_ed_df = ip_ed_df.groupby(['RecipientID','ServiceFromDt'],
                                    as_index=False).first()

        return ip_ed_df

    def risk_tier_calc(self, pat_ed_ip_df, risk_col_name):

        pat_ed_ip_df.loc[(pat_ed_ip_df['ED'] > 3) | (pat_ed_ip_df['IP'] > 1),
                         risk_col_name] = 'High'

        pat_ed_ip_df.loc[((pat_ed_ip_df['ED'] <= 3) & (pat_ed_ip_df['ED'] >= 1)) |
                         (pat_ed_ip_df['IP'] == 1), risk_col_name] = 'Medium'

        pat_ed_ip_df.loc[(pat_ed_ip_df['ED'] == 0) & (pat_ed_ip_df['IP'] == 0),
                         risk_col_name] = 'Low'

        return pat_ed_ip_df

    def risk_calc_window(self, pat_df_in, ed_ip_df, date_col, risk_col_name):
        '''Selects IP and ED bills that occured between the date_col and
            12 months back pat_df_in: pandas dataframe w/ columns ED, IP,
            ServiceFromDt, and date_col ed_ip_df: pandas dataframe'''

        window_size = 12
        pat_df = pat_df_in.copy()
        pat_df = pd.merge(pat_df, ed_ip_df, on='RecipientID', how='left')
        pat_df['Low_Window_Date'] = pat_df[date_col] - pd.DateOffset(months=window_size)
        pat_df['ServiceFromDt'] = pd.to_datetime(pat_df['ServiceFromDt'])
        pat_df[date_col] = pd.to_datetime(pat_df[date_col])

        pat_df = pat_df.loc[pat_df['ServiceFromDt'].between(pat_df['Low_Window_Date'],
                            pat_df[date_col])]

        pat_df_group = pat_df.groupby(['RecipientID','Category'])['Encounter'].count()
        pat_df_group = pat_df_group.unstack()

        #puts in patients that had no ED or IP visits in the time window if
        # they had a valid date
        re_ix = pat_df_in.loc[pat_df_in[date_col].notnull(), 'RecipientID']
        pat_df_group = pat_df_group.reindex(re_ix, fill_value=0)
        pat_df_group.fillna(0, inplace=True)
        pat_df_group = self.risk_tier_calc(pat_df_group, risk_col_name)
        return pat_df_group

    def risk_inserter(self, risk_df):

        self.connection.query('delete from pat_info_risk where ReleaseNum = {};'.format(self.release_num))

        insert_sql_str = '''insert into pat_info_risk
        (RecipientID,
        ReleaseNum,
        Current_Risk,
        Engagement_Risk,
        Enrollment_Risk,
        Randomization_Risk,
        Program_Risk
        ) values (%s, %s, %s, %s, %s, %s, %s)'''

        risk_df = risk_df[['RecipientID', 'ReleaseNum', 'Current_Risk',
                           'Engagement_Risk', 'Enrollment_Risk',
                           'Randomization_Risk', 'Program_Risk']]

        self.connection.insert(insert_sql_str, risk_df)

        return 'Inserted'

    def risk_history(self, pat_info_df, ed_ip_df, release_info_dict):

        total_risk_df = pat_info_df[['RecipientID']].copy()

        for release in load_release_info:

            pat_info_df[release['ReleaseNum']] = release['ReleaseDate']
            pat_info_df[release['ReleaseNum']] = pd.to_datetime(pat_info_df[release['ReleaseNum']])

            risk_col_name = str(x['ReleaseNum'])+'_Risk'

            risk_df = risker.risk_calc_window(pat_info_df, ip_ed_df, release['ReleaseNum'], risk_col_name)
            risk_df = risk_df.reset_index()

            total_risk_df = pd.merge(total_risk_df,
                                     risk_df[['RecipientID', risk_col_name]],
                                     on='RecipientID', how='left')

        return total_risk_df


    def risk_wrapper(self, insert=False):

        pat_info_df = self.pat_info_query()
        ip_ed_df = self.ip_ed_query()

        total_risk_df = pat_info_df[['RecipientID']]

        for date_col, risk_col in self.risk_date_columns.items():

            risk_df = self.risk_calc_window(pat_info_df, ip_ed_df,
                                            date_col, risk_col)
            risk_df = risk_df.reset_index()

            total_risk_df = pd.merge(total_risk_df, risk_df[['RecipientID',risk_col]],
                                     on='RecipientID', how='left')

        total_risk_df['ReleaseNum'] = self.release_num
        if insert == False:
            return total_risk_df
        else:
            return self.risk_inserter(total_risk_df)
