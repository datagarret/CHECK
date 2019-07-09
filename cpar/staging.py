from datetime import datetime
from CHECK.dbconnect import dbconnect


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
                        tbl.PK = mc.PK
                    where tbl.PK is null;'''.format(table)

        return sql_str

    def pk_update_all(self):

        for table in self.update_tables:
            if table != 'main_claims':
                table = 'raw_' + table
                update_pk_string = self.pk_update_str(table)
                output = self.connection.query(update_pk_string, output_format='none')
        return 'All PK in tables updated'

    def adjustment_delete_str(self, table):
        adj_str = '''DELETE tbl from {} tbl INNER JOIN
                raw_adjustments mc on tbl.DCN = mc.DCN
                AND tbl.ServiceLineNbr = mc.ServiceLineNbr
                AND tbl.RejectionStatusCd = mc.RejectionStatusCd
                AND tbl.RecipientID = mc.RecipientID
                AND tbl.AdjudicatedDt = mc.AdjudicatedDt where VoidInd = 'Y';'''.format(table)
        return adj_str

    def pk_table_maker(self):

        insert_str = '''INSERT into temp_pk_tbl (PK, DCN, ServiceLineNbr,
                        RejectionStatusCd, RecipientID, AdjudicatedDt)
                        (SELECT raw.PK, raw.DCN, raw.ServiceLineNbr,
                        raw.RejectionStatusCd, raw.RecipientID, raw.AdjudicatedDt
                        from raw_main_claims raw
                            LEFT JOIN temp_pk_tbl pk
                        on raw.PK=pk.PK
                       WHERE pk.PK is null and raw.RejectionStatusCd = 'N')'''
        insert_output = self.connection.query(insert_str, output_format='none')

        adjust_str = self.adjustment_delete_str('temp_pk_tbl')
        output = self.connection.query(adjust_str, output_format='none')

        return 'PK temp has been updated created'

    def raw_to_stage_str(self, raw_table, stage_table):
        sql = '''INSERT ignore into  {}
        select tbl.* from {} tbl inner join temp_pk_tbl mc
        on tbl.DCN = mc.DCN
        AND tbl.ServiceLineNbr = mc.ServiceLineNbr
        AND tbl.RejectionStatusCd = mc.RejectionStatusCd
        AND tbl.RecipientID = mc.RecipientID
        AND tbl.AdjudicatedDt = mc.AdjudicatedDt;'''.format(stage_table, raw_table)
        return sql

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
            raw_to_stage_sql = self.raw_to_stage_str(raw_table, stage_table)
            stage_insert = self.connection.query(raw_to_stage_sql,
                                                 output_format='none',
                                                 count_output=True)
            clean_sql = self.stage_clean(stage_table)
            stage_removed = self.connection.query(clean_sql,
                                                  output_format='none',
                                                  count_output=True)

            stage_insert_values.append([stage_table, self.release_num, load_date, stage_insert, 'Insert'])
            stage_insert_values.append([stage_table, self.release_num, load_date, stage_removed, 'Delete'])

            print('Table {} inserted {} rows deleted {} into stage'.format(table, stage_insert, stage_removed))

        return stage_insert_values


    def adjusted_price_update(self, table):
        update_str = '''UPDATE {} set AdjustedPriceAmt = GREATEST(NetLiabilityAmt,
                                                                  EncounterPriceAmt);'''.format(table)
        output = self.connection.query(update_str, output_format='none')

        update_str = '''update {} tbl inner join raw_adjustments mc on
                    tbl.DCN = mc.DCN
                    AND tbl.ServiceLineNbr = mc.ServiceLineNbr
                    AND tbl.RejectionStatusCd = mc.RejectionStatusCd
                    AND tbl.RecipientID = mc.RecipientID
                    AND tbl.AdjudicatedDt = mc.AdjudicatedDt set
                    AdjustedPriceAmt = CorrectedNetLiabilityAmt
                                    where VoidInd = '';'''.format(table)

        output = self.connection.query(update_str, output_format='none')



        return 'AdjustedPriceAmt Completed for {}'.format(table)



    def raw_to_stage_wrapper(self):

        print(self.pk_update_all())
        print(self.pk_table_maker())
        print(self.raw_to_stage_pharm_insert())
        stage_row_counts = self.raw_to_stage_insert_all()


        for table in ['stage_pharmacy','stage_main_claims']:
            self.adjusted_price_update(table)

        return stage_row_counts
