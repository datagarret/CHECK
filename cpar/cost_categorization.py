import numpy as np
import pandas as pd
from CHECK.dbconnect import dbconnect


class CostCategorizer():

    def __init__(self, db):
        self.connection = dbconnect.DatabaseConnect(db)
        self.nips_cat_df = pd.read_csv('/Users/check/.ipython/CHECK/cpar/categorization_files/nips_cat.csv')
        self.op_hcpcs_df = pd.read_csv('/Users/check/.ipython/CHECK/cpar/categorization_files/op_hcpcs_codes_cat.csv')
        self.op_rev_df = pd.read_csv('/Users/check/.ipython/CHECK/cpar/categorization_files/op_rev_codes_cat.csv',
                                     dtype={'RevenueCd':'str'})
        self.check_cat_df = pd.read_csv('/Users/check/.ipython/CHECK/cpar/categorization_files/check_category.csv')


    def categorize_wrapper(self):

        print('truncate')
        self.connection.query(self.truncate_category_sql(), output_format='none')

        categorization_info = []

        nips_cat = self.categorize_nips()
        if nips_cat is not None:
            insert_count = self.category_inserter(nips_cat)
            print('NIPS Categorize: {}'.format(insert_count))
        else:
            insert_count =  0

        op_cat = self.categorize_op()
        if op_cat is not None:
            insert_count = self.category_inserter(op_cat)
            print('OP Categorize: {}'.format(insert_count))
        else:
            insert_count = 0

        ip_cat = self.categorize_ip()
        if ip_cat is not None:
            insert_count = self.category_inserter(ip_cat)
            print('IP Categorize: {}'.format(insert_count))
        else:
            insert_count = 0

        print('Updating stage_main_claims...')
        update_count = self.connection.query(self.update_category_query(),
                                             output_format='none', count_output=True)

        return categorization_info



    def categorize_op(self):
        raw_op_data = self.connection.query(self.op_query(), output_format='df')
        if len(raw_op_data) == 0:
            print('No OP claims to categorize')
            return None
        op_cat = self.op_categorizer(raw_op_data)
        op_cat = pd.merge(op_cat, self.check_cat_df,
                          on=['Category1','Category2','Category3'], how='left')
        return op_cat


    def categorize_nips(self):
        raw_nip_data = self.connection.query(self.nips_query(), output_format='df')
        if len(raw_nip_data) == 0:
            print('No NIPS claims to categorize')
            return None
        nips_cat = self.nips_categorizer(raw_nip_data)
        nips_cat = pd.merge(nips_cat, self.check_cat_df,
                            on=['Category1','Category2','Category3'], how='left')
        return nips_cat


    def categorize_ip(self):
        raw_ip_data = self.connection.query(self.ip_query(), output_format='df')
        if len(raw_ip_data) == 0:
            print('No IP claims to categorize')
            return None
        raw_ip_data['UBTypeofBillCd'] = raw_ip_data['UBTypeofBillCd'].astype(int)
        ip_cat = self.ip_categorizer(raw_ip_data)
        ip_cat = pd.merge(ip_cat, self.check_cat_df,
                          on=['Category1','Category2','Category3'], how='left')
        return ip_cat


    def category_inserter(self, category_df):

        insert_sql_str = '''insert into tmp_cat_tbl
        (PK,
        Category1,
        Category2,
        Category3,
        CHECK_Category,
        Visit,
        Service_Count,
        Procedure_Count,
        Visit_Inpatient_Days) values (%s, %s, %s, %s,
        %s, %s, %s, %s, %s)'''

        category_df = category_df[['PK', 'Category1', 'Category2', 'Category3',
                                   'CHECK_Category', 'Visit', 'Service_Count',
                                   'Procedure_Count', 'Visit_Inpatient_Days']]

        insert_count = self.connection.insert(insert_sql_str, category_df)
        return insert_count

    def nips_query(self):
        sql_str = '''SELECT
        mc.PK,
        ProcCd,
        PlaceOfServiceCd
    FROM
        CHECK_CPAR2.stage_main_claims mc
            INNER JOIN
        stage_nips ni ON ni.PK = mc.PK
            INNER JOIN
        stage_procedure pr ON pr.PK = ni.PK
        WHERE MC.Category1 is NULL;'''
        return sql_str

    def ip_query(self):

        sql_str = '''SELECT
                mc.PK,
                AdmissionDt,
                DischargeDt,
                AdmissionSourceCd,
                ServiceFromDt,
                ServiceThruDt,
                UBTypeofBillCd,
                InpatientAdmissions,
                CoveredDays
            FROM
                CHECK_CPAR2.stage_main_claims mc
                    INNER JOIN
                stage_institutional ip ON ip.PK = mc.PK
            where Category1 is null and RecordIDCd = 'I';'''

        return sql_str

    def op_query(self):
        sql_str = '''SELECT
            mc.PK, rev.RevenueCd, RevenueHCPCSCd
        FROM
            CHECK_CPAR2.stage_main_claims mc
                INNER JOIN
            stage_institutional ins ON ins.PK = mc.PK
                left join
            stage_revenue_codes rev on rev.PK = mc.PK
        where Category1 is null and RecordIDCd = 'O';'''

        return sql_str

    def nips_categorizer(self, nips_cat):

        nips_cat = pd.merge(nips_cat, self.nips_cat_df, on='ProcCd', how='left')
        nips_cat.loc[:, 'Category1'] = 'NIPS'
        nips_cat.loc[:, 'Category3'] = np.nan

        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['PlaceOfServiceCd']=='A'), 'Category3'] = 'OFFICE_VISIT'
        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['PlaceOfServiceCd']=='C'), 'Category3'] = 'HOSPITAL_OUTPATIENT_VISIT'
        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['PlaceOfServiceCd']=='F'), 'Category3'] = 'RURAL_HEALTH_CLINIC_VISIT'
        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['PlaceOfServiceCd']=='K'), 'Category3'] = 'HOME_VISIT'

        phone_visit_cpt = ['99441', '99442', '99443', '99444', '99445',
                           '99446', '99447', '99448', '99449', 'G0425',
                           'G0426', 'G0427']

        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['ProcCd'].isin(phone_visit_cpt)), 'Category3'] = 'TELEPHONE_VISIT'

        nips_cat.loc[(nips_cat['Category2']=='OUTPATIENT_VISIT')&
                     (nips_cat['Category3'].isnull()), 'Category3'] = 'OTHER_OUTPATIENT_VISIT'

        nips_cat.loc[(nips_cat['Category2']=='MENTAL_COMMUNITY_HEALTH')&
                     (nips_cat['ProcCd'].str.startswith('H')), 'Category3'] = 'COMMUNITY_HEALTH_PROF'

        nips_cat.loc[(nips_cat['Category2']=='MENTAL_COMMUNITY_HEALTH')&
                     (nips_cat['Category3'].isnull()), 'Category3'] = 'MENTAL_HEALTH_PROF'


        nips_cat.loc[(nips_cat['Category2']=='MENTAL_COMMUNITY_HEALTH'),'Category3'].value_counts()


        nips_cat.loc[(nips_cat['Category2']=='CONSULTATION')&
                     (nips_cat['PlaceOfServiceCd']=='A'), 'Category3'] = 'OFFICE_CONSULT'

        nips_cat.loc[(nips_cat['Category2']=='CONSULTATION')&
                     (nips_cat['PlaceOfServiceCd']=='B'), 'Category3'] = 'INPATIENT_CONSULT'

        nips_cat.loc[(nips_cat['Category2']=='CONSULTATION')&
                     (nips_cat['PlaceOfServiceCd']=='C'), 'Category3'] = 'OUTPATIENT_CONSULT'

        nips_cat.loc[(nips_cat['Category2']=='CONSULTATION')&
                     (nips_cat['Category3'].isnull()), 'Category3'] = 'OTHER_CONSULT'


        nips_cat.loc[nips_cat['Category2'].isnull(),'Category2'] = 'OTHER'

        nip_prof_cats = ['LABORATORY', 'EMERGENCY', 'INPATIENT', 'RADIOLOGY',
                         'IMMUNIZATION', 'OTHER', 'SURGERY']

        for cat in nip_prof_cats:
            nips_cat.loc[nips_cat['Category2']==cat,'Category3'] = cat + '_PROF'

        nips_cat.loc[nips_cat['Category3'].isnull(),
                     'Category3'] = nips_cat.loc[nips_cat['Category3'].isnull(),'Category2']

        nips_cat.loc[:, 'Encounter'] = 1
        nips_cat.loc[:, 'Procedure_Count'] = 0
        nips_cat.loc[:, 'Visit_Inpatient_Days'] = 0
        nips_cat.loc[:, 'Service_Count'] = 0


        return nips_cat

    def ip_categorizer(self, ip_cat):

        ip_cat['Category1'] = 'INPATIENT'
        ip_cat['Category2'] = np.nan
        ip_cat['Category3'] = np.nan

        cat2_ip = [111, 112, 113, 114, 115, 116, 117, 118, 119, 121,
                   122, 123, 124, 125, 126, 127, 128, 129]
        cat2_snf = [211,212,213,214,215,216,217,218,219,221,222,223,
                    224,225,226,227,228,229,231,232,233,234,235,236,
                    237,238,239,241,242,243,244,245,246,247,248,249,
                    251,252,253,254,255,256,257,258,259,261,262,263,
                    264,265,266,267,268,269,271,272,273,274,275,276,
                    277,278,279,281,282,283,284,285,286,287,288,289]
        cat2_hsp = [811,812,813,814,815,816,817,818,819,821,822,823,
                    824,825,826,827,828,829]

        ip_cat.loc[ip_cat['UBTypeofBillCd'].isin(cat2_ip),'Category2'] = 'INPATIENT'
        ip_cat.loc[ip_cat['UBTypeofBillCd'].isin(cat2_snf),'Category2'] = 'SNF'
        ip_cat.loc[ip_cat['UBTypeofBillCd'].isin(cat2_hsp),'Category2'] = 'HOSPICE'
        ip_cat.loc[ip_cat['Category2'].isnull(),'Category2'] = 'OTHER'

        ip_services = self.connection.query(self.service_query('I'), output_format='df')

        ip_ed_pk_list = ip_services.loc[ip_services['RevenueCd'].isin(['0450','0451','0452',
                                                                       '0456','0459','0981']), 'PK']


        ip_cat.loc[ip_cat['PK'].isin(ip_ed_pk_list),'Category3'] = 'EMERGENCY_IP'

        ip_cat.loc[(ip_cat['Category2']=='INPATIENT')&
                   (ip_cat['Category3']!='EMERGENCY_IP'), 'Category3'] = 'INPATIENT_IP'

        ip_cat.loc[ip_cat['Category3'].isnull(),
                   'Category3'] =  ip_cat.loc[ip_cat['Category3'].isnull(),'Category2'] +'_IP'


        # Service Counts
        ip_service_counts = ip_services.groupby('PK', as_index=False)[['RevenueCd']].count()
        ip_service_counts = ip_service_counts.rename(columns={'RevenueCd':'Service_Count'})

        ip_cat = pd.merge(ip_cat, ip_service_counts, on='PK', how='left')

        ip_cat.loc[:, 'Service_Count'] = ip_cat['Service_Count'].fillna(0).astype(int)

        # Procedure Counts
        proc_counts = self.connection.query(self.procedure_count_query('I'), output_format='df')

        ip_cat = pd.merge(ip_cat, proc_counts, on='PK', how='left')
        ip_cat.loc[:, 'Procedure_Count'] = ip_cat['Procedure_Count'].fillna(0).astype(int)


        ip_cat['Encounter'] = 1
        ip_cat['Visit_Inpatient_Days']= ((ip_cat['ServiceThruDt'] -
                                          ip_cat['ServiceFromDt']).dt.days)+1

        return ip_cat

    def op_categorizer(self, op_cat):

        op_cat = pd.merge(op_cat, self.op_rev_df, on='RevenueCd', how='left')

        op_cat['Category1'] = op_cat['Category1'].fillna('OUTPATIENT')
        op_cat['Category2'] = op_cat['Category2'].fillna('UNCLASSIFIED')
        op_cat['Category3'] = op_cat['Category3'].fillna('UNCLASSIFIED')
        op_cat['Category2Rank'] = op_cat['Category2Rank'].fillna(15)
        op_cat['Category3Rank'] = op_cat['Category3Rank'].fillna(15)

        op_hcpcs = self.op_hcpcs_df.loc[self.op_hcpcs_df['Category3']=='OUTPATIENT_OP','RevenueHCPCSCd']
        op_cat.loc[op_cat['RevenueHCPCSCd'].isin(op_hcpcs),
                   ['Category2', 'Category3','Category2Rank','Category3Rank']] = ['OUTPATIENT', 'OUTPATIENT_OP', 3, 3]

        amb_hcpcs = self.op_hcpcs_df.loc[self.op_hcpcs_df['Category3']=='AMBULANCE_OP','RevenueHCPCSCd']
        op_cat.loc[op_cat['RevenueHCPCSCd'].isin(amb_hcpcs),
                  ['Category2', 'Category3','Category2Rank','Category3Rank']] = ['ANCILLARY',  'AMBULANCE_OP', 8, 8]

        dme_hcpcs = self.op_hcpcs_df.loc[self.op_hcpcs_df['Category3']=='DME_OP','RevenueHCPCSCd']
        op_cat.loc[op_cat['RevenueHCPCSCd'].isin(dme_hcpcs),
                   ['Category2', 'Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'DME_OP', 9, 9]

        hh_hcpcs = self.op_hcpcs_df.loc[self.op_hcpcs_df['Category3']=='HOME_HEALTH_OP','RevenueHCPCSCd']
        op_cat.loc[op_cat['RevenueHCPCSCd'].isin(hh_hcpcs),
                  ['Category2', 'Category3','Category2Rank','Category3Rank']] = ['ANCILLARY', 'HOME_HEALTH_OP', 10, 10]

        op_cat = op_cat.sort_values(['PK','Category2Rank','Category3Rank'])


        op_cat = op_cat.groupby('PK', as_index=False).agg({'Category1':'first','Category2':'first',
                                                           'Category3':'first', 'RevenueCd':'count'})

        op_cat = op_cat.rename(columns={'RevenueCd': 'Service_Count'})
        op_cat.loc[op_cat['Service_Count']!=1,
                   'Service_Count'] =  op_cat.loc[op_cat['Service_Count']!=1, 'Service_Count'] - 1

        op_cat.loc[:, 'Procedure_Count'] = 0
        op_cat.loc[:, 'Encounter'] = 1
        op_cat.loc[:, 'Visit_Inpatient_Days'] = 0

        return op_cat

    def service_query(self, category_type):

        sql_str = '''SELECT
            mc.PK, rev.RevenueCd, RevenueHCPCSCd
        FROM
            CHECK_CPAR2.stage_main_claims mc
                INNER JOIN
            stage_institutional ins ON ins.PK = mc.PK
                inner join
            stage_revenue_codes rev on rev.PK = mc.PK
        where Category1 is null and RecordIDCd = '{}' and RevenueCd != '0001';
        '''.format(category_type)

        return sql_str

    def procedure_count_query(self, category_type):
        '''category_type (str) ('I' or 'O')'''

        sql_str = '''SELECT
            mc.PK, count(ProcCd) Procedure_Count
        FROM
            CHECK_CPAR2.stage_main_claims mc
                INNER JOIN
            stage_institutional ins ON ins.PK = mc.PK
                inner join
            stage_procedure pr on pr.PK = mc.PK
        where Category1 is null and RecordIDCd = '{}'
        group by PK'''.format(category_type)
        return sql_str

    def update_category_query(self):
        update_sql_str = '''update stage_main_claims mc inner join tmp_cat_tbl cat on cat.PK = mc.PK
        set mc.Category1 = cat.Category1,
        mc.Category2 = cat.Category2,
        mc.Category3 = cat.Category3,
        mc.Visit = cat.Visit,
        mc.Service_Count = cat.Service_Count,
        mc.Procedure_Count = cat.Procedure_Count,
        mc.Visit_Inpatient_Days = cat.Visit_Inpatient_Days,
        mc.CHECK_Category = cat.CHECK_Category;'''

        return update_sql_str

    def truncate_category_sql(self):
        return 'truncate tmp_cat_tbl'
