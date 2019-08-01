import pandas as pd
from CHECK.dbconnect import dbconnect


class Finalizer():

    def __init__(self, database, release_num):

        self.connection = dbconnect.DatabaseConnect(database)
        self.release_num = release_num

    def premature_flag_update(self):

        sql  = '''update stage_main_claims mc inner join (
                    select RecipientID, DOB from pat_info_complete
                    where Prematurity = 1) pre
                    on pre.RecipientID = mc.RecipientID
                    set Prematurity_Flag = 1
                    where ServiceFromDt <= DATE_ADD(DOB, INTERVAL 90 DAY)'''

        self.connection.query(sql)

    def total_claims_truncate(self, query=True):
        sql = 'truncate total_claims;'
        if query == True:
            self.connection.query(sql)
            return 'Truncated total_claims'
        else:
            return sql

    def total_claims_pharm_insert(self, query=True):
        '''inserts pharmacy'''

        sql = '''INSERT into total_claims (PK, RecipientID, DCN, ServiceLineNbr,
        ServiceFromDt, ServiceThruDt, Category1, Category2, Category3,
        CHECK_Category, AdjustedPriceAmt, ReleaseNum)
        SELECT (ID * -1), RecipientID, DCN, ServiceLineNbr, ServiceFromDt,
        ServiceFromDt as ServicethruDt, 'PHARMACY' as Category1,
        'PHARMACY' Category2, 'PHARMACY' Category3, 'PHARMACY' CHECK_Category,
        AdjustedPriceAmt, ReleaseNum from stage_pharmacy;'''
        if query == True:
            self.connection.query(sql)
            return 'Inserted Pharmacy claims into total_claims'
        else:
            return sql

    def total_claims_main_insert(self, query=True):
        '''Inserts main claims that were not flagged as premature'''

        sql = '''INSERT INTO total_claims select PK, RecipientID, 0 as Month_Window,
        DCN, ServiceLineNbr, ServiceFromDt, ServicethruDt,
        Category1, Category2, Category3, CHECK_Category, AdjustedPriceAmt,
        Visit, Service_Count, Procedure_Count, Encounter,
        Visit_Inpatient_Days, Prematurity_Flag, ReleaseNum from stage_main_claims
        where Prematurity_Flag = 0;'''
        if query == True:
            self.connection.query(sql)
            return 'Inserted main claims into total_claims'
        else:
            return sql

    def total_claims_delete_oldest(self, query=True):
        '''Deletes bills that are only immunization, CCCD gives 2 years of most
        recent bills and 8 years of immunization. This removes bills that are greater
        than the two years.'''

        sql = '''DELETE tc FROM total_claims tc INNER JOIN
        pat_info_complete pic on pic.RecipientID = tc.RecipientID inner join
        load_release_info li on pic.Initial_Release = li.ReleaseNum
        where tc.ServiceFromDt < DATE_SUB(ReleaseDate, INTERVAL 2 YEAR);'''
        if query == True:
            self.connection.query(sql)
            return 'Deleted old claims from total_claims'
        else:
            return sql

    def total_claims_window_calc(self, query=True):
        '''Calculates the amount of months the claim is from the patients
           program date'''

        sql = '''UPDATE total_claims tc INNER JOIN
        pat_info_complete pic on tc.RecipientID = pic.RecipientID
        SET Month_Window = floor(DATEDIFF(tc.ServiceFromDt, pic.Program_Date)/30);'''

        if query == True:
            self.connection.query(sql)
            return 'Calculated Month_Window'
        else:
            return sql

    def total_claims_wrapper(self):
        print(self.premature_flag_update())
        print(self.total_claims_truncate())
        print(self.total_claims_pharm_insert())
        print(self.total_claims_main_insert())
        print(self.total_claims_delete_oldest())
        print(self.total_claims_window_calc())
