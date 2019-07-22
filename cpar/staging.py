from datetime import datetime
from CHECK.dbconnect import dbconnect
from CHECK.dbconnect import import_helpers as ihelpers

class Staging(object):

    def __init__(self, database, release_num):

        self.update_tables = ['nips', 'diagnosis', 'institutional',
                              'procedure', 'revenue_codes','main_claims']
        self.connection = dbconnect.DatabaseConnect(database)
        self.release_num = release_num


    def pk_update_str(self, table):
        sql_str = '''UPDATE {} tbl
                    INNER JOIN raw_main_claims mc on tbl.DCN = mc.DCN
                            AND tbl.ServiceLineNbr = mc.ServiceLineNbr
                            AND tbl.RejectionStatusCd = mc.RejectionStatusCd
                            AND tbl.RecipientID = mc.RecipientID
                            AND tbl.AdjudicatedDt = mc.AdjudicatedDt
                    SET
                        tbl.PK = mc.PK where tbl.PK is null;'''.format(table)

        return sql_str

    def pk_update_all(self):

        for table in self.update_tables + ['adjustments']:
            if table != 'main_claims':
                table = 'raw_' + table
                update_pk_string = self.pk_update_str(table)
                output = self.connection.query(update_pk_string, output_format='none')
        return 'All PK in tables updated'

    def adjustment_delete_str(self, table):
        if table == 'stage_pharmacy':
            adj_str = '''DELETE tbl from {} tbl INNER JOIN
                    raw_adjustments mc on tbl.DCN = mc.DCN
                    AND tbl.ServiceLineNbr = mc.ServiceLineNbr
                    AND tbl.RejectionStatusCd = mc.RejectionStatusCd
                    AND tbl.RecipientID = mc.RecipientID
                    AND tbl.AdjudicatedDt = mc.AdjudicatedDt where VoidInd = 'Y';'''.format(table)
        else:
            adj_str = '''DELETE tbl from {} tbl INNER JOIN
                    raw_adjustments mc on tbl.PK = mc.PK
                     where VoidInd = 'Y';'''.format(table)
        return adj_str


    def raw_to_stage_str(self, raw_table, stage_table, release_num):

        raw_table_cols = ihelpers.get_tbl_columns_query(self.connection,
                                                        raw_table)
        stage_table_cols = ihelpers.get_tbl_columns_query(self.connection,
                                                         stage_table)
        shared_cols = ihelpers.get_shared_columns(raw_table_cols,
                                                  stage_table_cols)

        insert_sql_str = ihelpers.insert_sql_generator(shared_cols,
                                                       import_tbl=stage_table,
                                                       export_tbl=raw_table,
                                                       insert_ignore=True)

        join_sql = ''' inner join raw_main_claims mc
        on et.DCN = mc.DCN
        AND et.ServiceLineNbr = mc.ServiceLineNbr
        AND et.RejectionStatusCd = mc.RejectionStatusCd
        AND et.RecipientID = mc.RecipientID
        AND et.AdjudicatedDt = mc.AdjudicatedDt
        where mc.RejectionStatusCd = 'N' and mc.ReleaseNum = {}'''.format(release_num)

        total_sql = insert_sql_str + join_sql

        return total_sql

    def stage_clean(self, stage_table):
        sql = '''DELETE stage
                  FROM {} stage
                  LEFT JOIN temp_pk_tbl pk
                     on stage.PK=pk.PK
                where pk.PK is null;'''.format(stage_table)
        return sql

    def raw_to_stage_pharm_insert(self):
        sql = '''INSERT ignore into stage_pharmacy select * FROM
        raw_pharmacy;'''
        output = self.connection.query(sql, output_format='none')

        adjust_str = self.adjustment_delete_str('stage_pharmacy')
        output = self.connection.query(adjust_str, output_format='none')
        return 'Pharmacy Staged'


    def raw_to_stage_insert_all(self):

        load_date = '{:%Y-%m-%d}'.format(datetime.today())

        stage_insert_values = []

        for table in self.update_tables:

            raw_table = 'raw_' + table
            stage_table = 'stage_' + table
            raw_to_stage_sql = self.raw_to_stage_str(raw_table, stage_table, self.release_num)
            nrows_stage_insert = self.connection.query(raw_to_stage_sql,
                                                 output_format='none',
                                                 count_output=True)
            clean_sql = self.adjustment_delete_str(stage_table)
            nrows_stage_removed = self.connection.query(clean_sql,
                                                  output_format='none',
                                                  count_output=True)

            stage_insert_values.append({'table':stage_table, 'release_num':self.release_num,
                                        'load_date':load_date, 'nrows': nrows_stage_insert,
                                        'type':'Insert'})

            stage_insert_values.append({'table':stage_table, 'release_num':self.release_num,
                                        'load_date':load_date, 'nrows': nrows_stage_removed,
                                        'type':'Delete'})

            print('Table {} inserted {} rows deleted {} into stage'.format(table, nrows_stage_insert, nrows_stage_removed))

        return stage_insert_values


    def adjusted_price_update(self, table):
        update_str = '''UPDATE {} set AdjustedPriceAmt = GREATEST(NetLiabilityAmt,
                                                                  EncounterPriceAmt);'''.format(table)
        output = self.connection.query(update_str, output_format='none')

        #some immunization bills from the first release have encounters at
        #the following costs. They are certaintly errors.
        update_str = '''UPDATE stage_main_claims set AdjustedPriceAmt = 0
        where (EncounterPriceAmt = 9999.99 or EncounterPriceAmt = 99999.99)
        and CatgofServiceCd=30;'''.format(table)
        output = self.connection.query(update_str, output_format='none')

        if table == 'stage_pharmacy':
            update_str = '''update {} tbl inner join raw_adjustments mc on
                        tbl.DCN = mc.DCN
                        AND tbl.ServiceLineNbr = mc.ServiceLineNbr
                        AND tbl.RejectionStatusCd = mc.RejectionStatusCd
                        AND tbl.RecipientID = mc.RecipientID
                        AND tbl.AdjudicatedDt = mc.AdjudicatedDt set
                        AdjustedPriceAmt = CorrectedNetLiabilityAmt
                                        where VoidInd = '';'''.format(table)
        else:
            update_str = '''update {} tbl inner join raw_adjustments mc on
                                tbl.PK = mc.PK
                            set AdjustedPriceAmt = CorrectedNetLiabilityAmt
                            where VoidInd = '';'''.format(table)

        output = self.connection.query(update_str, output_format='none')

        return 'AdjustedPriceAmt Completed for {}'.format(table)

    def stage_diagnosis_update(self):
        '''adds ccs diagnosis categories into stage_diagnosis.'''

        sql_str = '''UPDATE stage_diagnosis sdx
                INNER JOIN
            zref_hcup_ccs_dx ccs ON sdx.DiagCd = ccs.DiagCd
                AND sdx.ICDVersion = ccs.ICDVersion
        SET
            sdx.CCSCategory = ccs.CCSCategory,
            sdx.ChronicIndicator = ccs.ChronicIndicator
        WHERE
            sdx.CCSCategory IS NULL;'''

        return sql_str


    def raw_to_stage_wrapper(self):

        print(self.pk_update_all())
        print(self.raw_to_stage_pharm_insert())
        stage_row_counts = self.raw_to_stage_insert_all()


        for table in ['stage_pharmacy','stage_main_claims']:
            self.adjusted_price_update(table)

        self.connection.query(self.stage_diagnosis_update(), output_format='none')

        return stage_row_counts
