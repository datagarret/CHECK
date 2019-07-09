import os
from datetime import datetime
from CHECK.dbconnect import dbconnect


class HFSLoadData(object):

    def __init__(self, database, release_num, file_path):

        self.raw_file_path = os.path.join(file_path,'Raw_Data')
        self.sql_file_path = os.path.join(file_path,'SQL_Scripts')

        self.info_dict = {'path': self.raw_file_path,
                          'adjustment': 'Adjustments',
                          'main_claims': 'Main_Claims',
                          'nips': 'NIPS',
                          'pharmacy': 'Pharmacy',
                          'procedure': 'Procedure',
                          'recipient_flags': 'Recipient_Flags',
                          'revenue': 'Revenue_Codes',
                          'compound_drug': 'Pharmacy_Prior_Authorization',
                          'immunization': 'Cornerstone_Immunization',
                          'diagnosis': 'Diagnosis',
                          'institutional': 'Institutional',
                          'lead': 'Lead',
                          'ending': ';\n\n'}

        self.info_dict['load_date'] = '{:%Y-%m-%d}'.format(datetime.today())
        self.info_dict['db'] = database
        self.info_dict['ReleaseNum'] = str(release_num).replace('\n','').strip()
        self.load_inline_dict = {}

        self.connection = dbconnect.DatabaseConnect(self.info_dict['db'])
        # gets last inserts release and adds one
        # current_releasenum = connection.query('SELECT MAX(ReleaseNum)
        # from pat_info_demo').values[0][0]
        # self.info_dict['ReleaseNum'] = str(current_releasenum)

    def renew_script(self, file_name, start_string=None):
        '''Rewrites a file if it exists and will add a
            header to the file with start_string'''
        try:
            os.remove(file_name)
        except:
            pass
        finally:
            if start_string is not None:
                text_file = open(file_name, "a")
                text_file.write(start_string)
                text_file.close()

    def sql_query_generate(self):
        '''Writes 3 sql scripts: An insert script, a delete script that
           removes the load from the raw tables if an error
           occurs and an insert scripts which records the
           numbers of rows into the hfs_load_count_info table
           The sql commands are stored in the dictionary and can be queried
           when the file_loader method is called'''

        self.load_inline_dict['adjustment_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{adjustment}'
          IGNORE INTO TABLE {db}.raw_adjustments
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RecipientID = TRIM(SUBSTR(@row,18,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
            CorrectedNetLiabilityAmt = nullif(TRIM(SUBSTR(@row,37,11)),''),
            DeltaNetLiabilityAmt = nullif(TRIM(SUBSTR(@row,48,11)),''),
            VoidInd = TRIM(SUBSTR(@row,59,1)),
            ReleaseNum = {ReleaseNum}{ending}"""
            .format(**self.info_dict),
            'file_path': '{path}/{adjustment}'.format(**self.info_dict),
            'table_name': 'raw_adjustments'}

        self.load_inline_dict['main_claims_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{main_claims}'
          IGNORE INTO TABLE {db}.raw_main_claims
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            ServiceFromDt = str_to_date(TRIM(SUBSTR(@row,38,10)), '%Y-%m-%d'),
            ServiceThruDt = str_to_date(TRIM(SUBSTR(@row,48,10)), '%Y-%m-%d'),
            CatgofServiceCd = TRIM(SUBSTR(@row,58,3)),
            RecordIDCd = TRIM(SUBSTR(@row,61,1)),
            ProviderID = TRIM(SUBSTR(@row,62,12)),
            ProviderTypeCd = TRIM(SUBSTR(@row,74,3)),
            DataTypeCd = TRIM(SUBSTR(@row,77,1)),
            DocumentCd = TRIM(SUBSTR(@row,78,2)),
            PayeeID = TRIM(SUBSTR(@row,80,16)),
            PriorApprovalCd = TRIM(SUBSTR(@row,96,1)),
            ProviderNPI = TRIM(SUBSTR(@row,97,10)),
            EncounterPriceAmt = nullif(TRIM(SUBSTR(@row,107,11)),''),
            NetLiabilityAmt = nullif(TRIM(SUBSTR(@row,118,11)),''),
            MedicareBillProviderTaxonomy = TRIM(SUBSTR(@row,129,10)),
            ProviderTaxonomy = TRIM(SUBSTR(@row,139,10)),
            ProviderChargeAmt = nullif(TRIM(SUBSTR(@row,149,11)),''),
            CopayAmt = nullif(TRIM(SUBSTR(@row,160,11)),''),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{main_claims}'.format(**self.info_dict),
            'table_name': 'raw_main_claims'}

        self.load_inline_dict['immunization_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{immunization}'
          IGNORE INTO TABLE {db}.raw_cornerstone_immunization
          (@row)
          SET
            RecipientID = TRIM(SUBSTR(@row,1,9)),
            ImmnDt = str_to_date(TRIM(SUBSTR(@row,10,10)), '%Y-%m-%d'),
            ImmnTyp = TRIM(SUBSTR(@row,20,4)),
            ImunzTypDesc = TRIM(SUBSTR(@row,24,40)),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{immunization}'.format(**self.info_dict),
            'table_name': 'raw_cornerstone_immunization'}

        self.load_inline_dict['lead_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{lead}'
          IGNORE INTO TABLE {db}.raw_lead
          (@row)
          SET
            RecipientID = TRIM(SUBSTR(@row,1,9)),
            LabNumber = TRIM(SUBSTR(@row,10,16)),
            CollectedDate = str_to_date(TRIM(SUBSTR(@row,26,10)), '%Y-%m-%d'),
            BirthDate = str_to_date(TRIM(SUBSTR(@row,36,10)), '%Y-%m-%d'),
            TestResult = TRIM(SUBSTR(@row,46,3)),
            TestType = TRIM(SUBSTR(@row,49,1)),
            ConfirmLevel = TRIM(SUBSTR(@row,50,3)),
            ConfirmDate = str_to_date(TRIM(SUBSTR(@row,53,10)), '%Y-%m-%d'),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{lead}'.format(**self.info_dict),
            'table_name': 'raw_lead'}

        self.load_inline_dict['pharmacy_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{pharmacy}'
          IGNORE INTO TABLE {db}.raw_pharmacy
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RecipientID = TRIM(SUBSTR(@row,18,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
            ServiceFromDt = str_to_date(TRIM(SUBSTR(@row,37,10)), '%Y-%m-%d'),
            CatgofServiceCd = TRIM(SUBSTR(@row,47,3)),
            RecordIDCd = TRIM(SUBSTR(@row,50,1)),
            ProviderID = TRIM(SUBSTR(@row,51,12)),
            ProviderTypeCd = TRIM(SUBSTR(@row,63,3)),
            DataTypeCd = TRIM(SUBSTR(@row,66,1)),
            DocumentCd = TRIM(SUBSTR(@row,67,2)),
            PayeeID = TRIM(SUBSTR(@row,69,16)),
            PriorApprovalCd = TRIM(SUBSTR(@row,85,1)),
            NationalDrugCd = TRIM(SUBSTR(@row,86,11)),
            DrugDaysSupplyNbr = nullif(TRIM(SUBSTR(@row,97,3)),''),
            DrugQuanAllow = nullif(TRIM(SUBSTR(@row,100,10)),''),
            DrugSpecificTherapeuticClassCd = TRIM(SUBSTR(@row,110,3)),
            PrimaryCareProviderID = TRIM(SUBSTR(@row,113,12)),
            ProviderNPI = TRIM(SUBSTR(@row,125,10)),
            PrescribingPractitionerId = TRIM(SUBSTR(@row,135,12)),
            PrescriptionNbr = TRIM(SUBSTR(@row,147,12)),
            CompoundCd = TRIM(SUBSTR(@row,159,1)),
            RefillNbr = TRIM(SUBSTR(@row,160,2)),
            NbrRefillsAuth = nullif(TRIM(SUBSTR(@row,162,2)),''),
            DrugDAWCd = TRIM(SUBSTR(@row,164,1)),
            PrescriptionDt = str_to_date(TRIM(SUBSTR(@row,165,10)), '%Y-%m-%d')
            ,PrescribingLastName = TRIM(SUBSTR(@row,175,15)),
            LabelName = TRIM(SUBSTR(@row,190,30)),
            GenericCdNbr = TRIM(SUBSTR(@row,220,5)),
            DrugStrengthDesc = TRIM(SUBSTR(@row,225,10)),
            GenericInd = TRIM(SUBSTR(@row,235,1)),
            GenericSequenceNbr = TRIM(SUBSTR(@row,236,6)),
            EncounterPriceAmt = nullif(TRIM(SUBSTR(@row,242,11)),''),
            NetLiabilityAmt = nullif(TRIM(SUBSTR(@row,253,11)),''),
            CopayAmt = nullif(TRIM(SUBSTR(@row,264,11)),''),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{pharmacy}'.format(**self.info_dict),
            'table_name': 'raw_pharmacy'}

        self.load_inline_dict['recipient_flags_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{recipient_flags}'
          IGNORE INTO TABLE {db}.raw_recipient_flags
          (@row)
          SET
            RecipientID = TRIM(SUBSTR(@row,1,9)),
            RecipientFlagCd = TRIM(SUBSTR(@row,10,2)),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{recipient_flags}'.format(**self.info_dict),
            'table_name': 'raw_recipient_flags'}

        self.load_inline_dict['diagnosis_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{diagnosis}'
          IGNORE INTO TABLE {db}.raw_diagnosis
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            DiagCd = TRIM(SUBSTR(@row,38,8)),
            PrimaryDiagInd = TRIM(SUBSTR(@row,46,1)),
            TraumaInd = TRIM(SUBSTR(@row,47,1)),
            DiagPrefixCd = TRIM(SUBSTR(@row,48,1)),
            POAClaimCd = TRIM(SUBSTR(@row,49,1)),
            ICDVersion = TRIM(SUBSTR(@row,50,2)),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{diagnosis}'.format(**self.info_dict),
            'table_name': 'raw_diagnosis'}

        self.load_inline_dict['institutional_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{institutional}'
          IGNORE INTO TABLE {db}.raw_institutional
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            BillTypeFrequencyCd = TRIM(SUBSTR(@row,38,1)),
            AdmissionSourceCd = TRIM(SUBSTR(@row,39,2)),
            AdmissionTypeCd = TRIM(SUBSTR(@row,41,1)),
            DRGGroupCd = TRIM(SUBSTR(@row,42,3)),
            PricingCd = TRIM(SUBSTR(@row,45,1)),
            AdmissionDt = str_to_date(TRIM(SUBSTR(@row,46,10)), '%Y-%m-%d'),
            DischargeDt = str_to_date(TRIM(SUBSTR(@row,56,10)), '%Y-%m-%d'),
            PatientStatusCd = TRIM(SUBSTR(@row,66,2)),
            ProviderDRGAssignedCd = TRIM(SUBSTR(@row,68,7)),
            UBTypeofBillCd = TRIM(SUBSTR(@row,75,3)),
            OutPatientAPLGrp = TRIM(SUBSTR(@row,78,2)),
            APLProcGroupCd = TRIM(SUBSTR(@row,80,5)),
            GrouperVersionNbr = TRIM(SUBSTR(@row,85,3)),
            SOICd = TRIM(SUBSTR(@row,88,1)),
            InpatientAdmissions = nullif(TRIM(SUBSTR(@row,89,3)),''),
            CoveredDays = nullif(TRIM(SUBSTR(@row,92,5)),''),
            AdmissionDiagCd = TRIM(SUBSTR(@row,97,8)),
            ICDVersion = TRIM(SUBSTR(@row,105,2)),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{institutional}'.format(**self.info_dict),
            'table_name': 'raw_institutional'}

        self.load_inline_dict['nips_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{nips}'
          IGNORE INTO TABLE {db}.raw_nips
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            PlaceOfServiceCd = TRIM(SUBSTR(@row,38,2)),
            ReferringPractitionerId = TRIM(SUBSTR(@row,40,12)),
            OriginatingPlaceCd = TRIM(SUBSTR(@row,52,3)),
            DestinationPlaceCd = TRIM(SUBSTR(@row,55,3)),
            AllowedUnitsQuan = nullif(TRIM(SUBSTR(@row,58,7)),''),
            TotalUnitsQuan = nullif(TRIM(SUBSTR(@row,65,10)),''),
            SpecialPhysicianNPI = TRIM(SUBSTR(@row,75,10)),
            SeqLineNbr = TRIM(SUBSTR(@row,85,2)),
            ReleaseNum = {ReleaseNum}{ending}""".format(**self.info_dict),
            'file_path': '{path}/{nips}'.format(**self.info_dict),
            'table_name': 'raw_nips'}

        self.load_inline_dict['compound_drug_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{compound_drug}'
          IGNORE INTO TABLE {db}.raw_compound_drugs_detail
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RecipientID = TRIM(SUBSTR(@row,18,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,27,10)), '%Y-%m-%d'),
            NationalDrugCd = TRIM(SUBSTR(@row,37,11)),
            CompoundDispUnitCd = TRIM(SUBSTR(@row,48,1)),
            CompoundDosageFormCd = TRIM(SUBSTR(@row,49,2)),
            IngrQuan = nullif(TRIM(SUBSTR(@row,51,10)),''),
            ReleaseNum = {ReleaseNum}{ending}"""
            .format(**self.info_dict),
            'file_path': '{path}/{compound_drug}'.format(**self.info_dict),
            'table_name': 'raw_compound_drugs_detail'}

        self.load_inline_dict['procedure_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{procedure}'
          IGNORE INTO TABLE {db}.raw_procedure
          (@row)
          SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            ProcCd = TRIM(SUBSTR(@row,38,8)),
            ProcPrefixCd = TRIM(SUBSTR(@row,46,1)),
            ProcDt = str_to_date(TRIM(SUBSTR(@row,47,10)), '%Y-%m-%d'),
            PrimaryProcInd = TRIM(SUBSTR(@row,57,1)),
            ProcModifierCd1 = TRIM(SUBSTR(@row,58,2)),
            ProcModifierCd2 = TRIM(SUBSTR(@row,60,2)),
            ProcModifierCd3 = TRIM(SUBSTR(@row,62,2)),
            ProcModifierCd4 = TRIM(SUBSTR(@row,64,2)),
            ICDVersion = TRIM(SUBSTR(@row,66,2)),
            SeqLineNbr = TRIM(SUBSTR(@row,68,2)),
            ReleaseNum = {ReleaseNum}{ending}"""
            .format(**self.info_dict),
            'file_path': '{path}/{procedure}'.format(**self.info_dict),
            'table_name': 'raw_procedure'}

        self.load_inline_dict['revenue_table'] = {'inline_load':
        """LOAD DATA INFILE '{path}/{revenue}'
           IGNORE INTO TABLE {db}.raw_revenue_codes
           (@row)
           SET
            DCN = TRIM(SUBSTR(@row,1,15)),
            ServiceLineNbr = TRIM(SUBSTR(@row,16,2)),
            RejectionStatusCd = TRIM(SUBSTR(@row,18,1)),
            RecipientID = TRIM(SUBSTR(@row,19,9)),
            AdjudicatedDt = str_to_date(TRIM(SUBSTR(@row,28,10)), '%Y-%m-%d'),
            RevenueCd = TRIM(SUBSTR(@row,38,4)),
            RevenueHCPCSCd = TRIM(SUBSTR(@row,42,8)),
            RevenueHCPCSMod1Cd = TRIM(SUBSTR(@row,50,2)),
            RevenueHCPCSMod2Cd = TRIM(SUBSTR(@row,52,2)),
            RevenueHCPCSMod3Cd = TRIM(SUBSTR(@row,54,2)),
            RevenueHCPCSMod4Cd = TRIM(SUBSTR(@row,56,2)),
            NDCNumber1 = TRIM(SUBSTR(@row,58,11)),
            NDCQuantity1 = nullif(TRIM(SUBSTR(@row,69,11)),''),
            NDCNumber2 = TRIM(SUBSTR(@row,80,11)),
            NDCQuantity2 = nullif(TRIM(SUBSTR(@row,91,11)),''),
            NDCNumber3 = TRIM(SUBSTR(@row,102,11)),
            NDCQuantity3 = nullif(TRIM(SUBSTR(@row,113,11)),''),
            RevenueNonCoveredChargeAmt = nullif(TRIM(SUBSTR(@row,124,11)),''),
            RevenueTotalChargeAmt = nullif(TRIM(SUBSTR(@row,135,11)),''),
            SeqLineNbr = TRIM(SUBSTR(@row,146,3)),
            EAPGCd = TRIM(SUBSTR(@row,149,5)),
            EAPGTypeCd = TRIM(SUBSTR(@row,154,2)),
            EAPGCatgCd = TRIM(SUBSTR(@row,156,2)),
            ReleaseNum = {ReleaseNum}{ending}"""
            .format(**self.info_dict),
            'file_path': '{path}/{revenue}'.format(**self.info_dict),
            'table_name': 'raw_revenue_codes'}

        self.sql_file_path

        mysql_script_name = '''Load_Data_to_DB_ReleaseNum_{ReleaseNum}.sql'''.format(**self.info_dict)
        insert_info_script_name = '''Load_info_{ReleaseNum}.sql'''.format(**self.info_dict)
        delete_info_script_name = '''Delete_Release_Info_{ReleaseNum}.sql'''.format(**self.info_dict)
        mysql_script_name = os.path.join(self.sql_file_path, mysql_script_name)
        insert_info_script_name = os.path.join(self.sql_file_path, insert_info_script_name)
        delete_info_script_name = os.path.join(self.sql_file_path, delete_info_script_name)

        self.renew_script(insert_info_script_name, "USE {db};\n".format(**self.info_dict))
        self.renew_script(delete_info_script_name, "USE {db};\n".format(**self.info_dict))
        self.renew_script(mysql_script_name)

        for key in self.load_inline_dict.keys():

            table = self.load_inline_dict[key]['table_name']
            insert_str = """INSERT INTO hfs_load_count_info(Table_Name,
                            ReleaseNum, Load_Date,
                            Count) select '{table}' as Table_Name, {ReleaseNum}
                            as ReleaseNum, '{load_date}' as Load_Date,
                            (select count(*) from {table} where ReleaseNum
                            = {ReleaseNum})
                            as Count;\n\n""".format(table=table,
                                                    **self.info_dict)
            delete_str = """DELETE FROM {table} WHERE releasenum =
                            {ReleaseNum};\n""".format(table=table,
                                                      **self.info_dict)

            self.load_inline_dict[key]['sql_insert'] = insert_str
            self.load_inline_dict[key]['sql_delete'] = delete_str
            self.line_append(mysql_script_name, self.load_inline_dict[key]['inline_load'])
            self.line_append(insert_info_script_name, insert_str)
            self.line_append(delete_info_script_name, delete_str)

    def line_append(self, script_name, str_append):
        '''adds line without having to perform open close over and over'''
        text_file = open(script_name, "a")
        text_file.write(str_append)
        text_file.close()

    def load_table_inline(self):
        '''loads data inline to raw tables and catalogs rows
           counts into hfs_load_count_info'''
        table_count = 12
        counter = 0

        for key in self.load_inline_dict.keys():
            table = self.load_inline_dict[key]['table_name']
            inline_str = self.load_inline_dict[key]['inline_load']
            file = self.load_inline_dict[key]['file_path']

            if os.path.exists(file):
                load_count = self.connection.inline_import(inline_str, None)
                counter += 1
                print('{}: Load completed correctly n={}'.format(table, load_count))
            else:
                print('{}: file is not present!!'.format(table))
                load_count = 0

            self.load_inline_dict[key]['sql_insert'] = """INSERT INTO
              hfs_load_count_info(TableName, ReleaseNum, LoadDate, NRowsImport)
              select '{table}' as TableName, {ReleaseNum} as ReleaseNum,
              '{load_date}' as LoadDate, {row_count} as NRowsImport;
              """.format(table=table, row_count=load_count, **self.info_dict)

        if table_count == counter:
            print('\nAll tables loaded into raw tables correctly\n')
        else:
            print('{}/{} tables loaded!!!'.format(counter, table_count))
        for key in self.load_inline_dict.keys():
            self.connection.query(self.load_inline_dict[key]['sql_insert'],
                                  df_flag=False)

    def delete_tbl_rows(self, table_key):
        '''table_key: (str) deletes rows with associated table_key.
            When set to 'All' deletes all rows that were inserted into
            tables and hfs_load_count_info'''
        if table_key == 'All':
            for key in self.load_inline_dict.keys():
                self.connection.query(self.load_inline_dict[key]['sql_delete'], df_flag=False)
                print('Deleting rows from {}'.format(self.load_inline_dict[key]['table_name']))

            self.connection.query("""DELETE FROM {db}.hfs_load_count_info
                                     where ReleaseNum = {ReleaseNum}"""
                                  .format(**self.info_dict), df_flag=False)
        else:
            self.connection.query(self.load_inline_dict[table_key]['sql_delete'], df_flag=False)

            print('Deleted rows from {}'.format(self.load_inline_dict[table_key]['table_name']))
