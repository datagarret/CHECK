import pandas as pd
from CHECK.dbconnect import dbconnect


class DiagnosisCategorizer(object):

    def __init__(self, database='CHECK_CPAR2'):

        #qualifying ratio i,e, 3 inclusion codes for every 1 exclusion code to be diagnosed
        self.dx_ratio = {'SCD': .75}

        self.connector = dbconnect.DatabaseConnect(database)

        self.diagnosis_tables = ['pat_info_dx_mental_health',
                                 'pat_info_dx_pregnancy',
                                 'pat_info_dx_primary']


    def pat_info_query(self):
        pat_info_sql = '''SELECT RecipientID, Program_Age, Gender,
                          Program_Date from pat_info_demo'''
        return self.connector.query(pat_info_sql, output_format='df')

    def dx_code_query(self):
        dx_code_query = '''SELECT mc.RecipientID, DiagCd,
        min(ServiceFromDt) Min_Date, count(*) ICD_Count
        from stage_diagnosis dx
        inner join
        stage_main_claims mc on mc.PK = dx.PK
        group by DiagCd, RecipientID;'''
        return self.connector.query(dx_code_query, output_format='df')

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
        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_primary_diagnosis;", output_format='df')
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)
        pt_dx_table.loc[pt_dx_table['Program_Age'] > 3, 'Prematurity'] = 0
        pt_dx_table.loc[:,'ICD_List'] = '1'
        pt_dx_table.loc[~(pt_dx_table['RecipientID'].isin(pt_dx_codes['RecipientID'])),'ICD_List'] = '0'
        pt_dx_table['Diagnosis_Category'] = pt_dx_table.apply(self.diagnosis_category, axis=1)
        return pt_dx_table

    def mh_dx(self, pt_dx_codes, pt_info):
        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_mental_health;", output_format='df')
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)
        return pt_dx_table

    def preg_dx(self, pt_dx_codes, pt_info):
        '''Determines if pregnancy icd code was ever given to patient
        Should only occur for Females over age 10 at time of enrollment'''

        inc_exc_table = self.connector.query( "SELECT * FROM dx_code_inc_exc_pregnancy;", output_format='df')
        pt_dx_table = self.dx_table_iterator(pt_info, pt_dx_codes, inc_exc_table)

        pt_dx_table.loc[(pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) > 0) &
                        (pt_dx_table['Program_Age']>10) & (pt_dx_table['Gender']=='Female'), 'Preg_Flag'] = 1
        pt_dx_table.loc[pt_dx_table[['Antenatal_care','Delivery','Abortive']].sum(axis=1) == 0, 'Preg_Flag'] = 0

        pt_dx_table.loc[:, 'Preg_Flag'] = pt_dx_table['Preg_Flag'].fillna(0).astype(int)
        return pt_dx_table

    def load_diag_data(self):
        '''Calculates for all of the diagnosis tables and if to_sql is True loads them into the
        database'''
        dx_dfs = {}
        pat_info = self.pat_info_query()
        dx_codes = self.dx_code_query()

        for table in self.diagnosis_tables:
            if table == 'pat_info_dx_primary':
                pat_dx_table = self.primary_dx(dx_codes, pat_info)
            elif table == 'pat_info_dx_pregnancy':
                pat_dx_table = self.preg_dx(dx_codes, pat_info)
            elif table == 'pat_info_dx_mental_health':
                pat_dx_table = self.mh_dx(dx_codes, pat_info)

            dx_dfs[table] = pat_dx_table

        return dx_dfs
