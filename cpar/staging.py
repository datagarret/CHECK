from CHECK.dbconnect import dbconnect


class Staging(object):

    def __init__(self, database):
        self.update_tables = ['nips', 'diagnosis', 'institutional',
                              'procedure', 'revenue_codes','main_claims']
        self.connection = dbconnect.DatabaseConnect(database)

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
                output = self.connection.query(update_pk_string, df_flag=False)
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
        drop_table_str = '''drop table if exists temp_pk_tbl;'''
        output = self.connection.query(drop_table_str, df_flag=False)
        create_table_str = '''CREATE TABLE temp_pk_tbl (
                              PK int(11) NOT NULL,
                              DCN char(15) DEFAULT NULL,
                              ServiceLineNbr char(2) DEFAULT NULL,
                              RejectionStatusCd char(1) DEFAULT NULL,
                              RecipientID char(9) DEFAULT NULL,
                              AdjudicatedDt date DEFAULT NULL,
                              PRIMARY KEY (PK),
                              UNIQUE KEY hfs_ix (DCN, ServiceLineNbr,
                              RejectionStatusCd, RecipientID, AdjudicatedDt))'''

        output = self.connection.query(create_table_str, df_flag=False)

        insert_str = '''INSERT INTO temp_pk_tbl select PK, DCN, ServiceLineNbr,
        RejectionStatusCd, RecipientID, AdjudicatedDt FROM raw_main_claims
        where RejectionStatusCd = 'N';'''
        insert_output = self.connection.query(insert_str, df_flag=False)

        adjust_str = self.adjustment_delete_str('temp_pk_tbl')
        output = self.connection.query(adjust_str, df_flag=False)

        return 'PK temp table created'

    def stage_clear(self, table):
        sql = '''truncate {};'''.format(table)
        return sql

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
        output = self.connection.query(sql, df_flag=False)

        adjust_str = self.adjustment_delete_str('stage_pharmacy')
        output = self.connection.query(adjust_str, df_flag=False)

        return 'Pharmacy Staged'

    def raw_to_stage_insert_all(self):

        for table in self.update_tables:
            raw_table = 'raw_' + table
            stage_table = 'stage_' + table
            clean_sql = self.stage_clean(stage_table)
            output = self.connection.query(clean_sql, df_flag=False)
            raw_to_stage_sql = self.raw_to_stage_str(raw_table, stage_table)
            output = self.connection.query(raw_to_stage_sql, df_flag=False)
            print('Table {} inserted into stage'.format(table))

        return 'Tables Inserted'


    def raw_to_stage_wrapper(self):
        print(self.pk_update_all())
        print(self.pk_table_maker())
        print(self.raw_to_stage_pharm_insert())
        print(self.raw_to_stage_insert_all())
