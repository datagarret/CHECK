import os
import zipfile
from CHECK.secret import secret


class ExtractFiles(object):

    def __init__(self, file_path):

        self.file_path = file_path
        self.raw_file_path = os.path.join(self.file_path, "Raw_Data")
        self.output_file_path = os.path.join(self.file_path, "Output_Data")
        self.sql_file_path = os.path.join(self.file_path, "SQL_Scripts")
        self._sec = secret.Secret()

        self.file_rename = {'recipientflags_final_uick1.out':'Recipient_Flags',
                            'cornerstone_finaluick1.out':'Cornerstone_Immunization',
                            'servicediag_finaluick1.out':'Diagnosis',
                            'lead_finaluick1.out':'Lead',
                            'servicepharmndc_finaluick1.out':'Pharmacy_Prior_Authorization',
                            'pharmacy_finaluick1.out':'Pharmacy',
                            'icare_finaluick1.out':'ICARE_Immunization_Data',
                            'servicenips_finaluick1.out':'NIPS',
                            'serviceinst_finaluick1.out':'Institutional',
                            'claim_finaluick1.out':'Main_Claims',
                            'serviceproc_finaluick1.out':'Procedure',
                            'servicerev_finaluick1.out':'Revenue_Codes',
                            'adjustedclaimextractuick1.out': 'Adjustments'}

    def file_initiator(self):

        if os.path.exists(self.raw_file_path) is not True:
            os.mkdir(self.raw_file_path)

        if os.path.exists(self.output_file_path) is not True:
            os.mkdir(self.output_file_path)

        if os.path.exists(self.sql_file_path) is not True:
            os.mkdir(self.sql_file_path)

        base_zip = self.zip_check(self.file_path)
        raw_zip = self.zip_check(self.raw_file_path)

        unzip_status = None

        rename_count = self.raw_file_rename()
        if rename_count >= 11 and rename_count <= 13:
            unzip_status = 'Previously Unzipped'

        elif base_zip['Status'] == False and raw_zip['Status'] == False:
            unzip_status = 'No zip or outputted data present.'

        elif base_zip['Status'] == True and raw_zip['Status'] == False:
            #move zip to raw data
            new_zip_path = os.path.join(self.raw_file_path, base_zip['File_Name'])
            os.rename(base_zip['Path'], new_zip_path)
            unzip_status = self.unzip_rename(new_zip_path)

        elif base_zip['Status'] == False and raw_zip['Status'] == True:
            unzip_status = self.unzip_rename(raw_zip['Path'])

        else:
            raise 'More than one zip file present!!! remove either to run again'
        return unzip_status


    def zip_check(self, path):
        zip_file = [i for i in os.listdir(path)
                    if i.endswith('CCCDMonthlyUICheck.zip')]
        if len(zip_file) == 0:
            return {'Status': False}
        elif len(zip_file) == 1:
            zip_path = os.path.join(path, zip_file[0])
            return {'Status': True, 'File_Name':zip_file[0], 'Path':zip_path}
        else:
            raise 'more than one zip file present!!'

    def unzip_files(self, zip_file):
        # unzip the file from source folder to output folder
        try:
            file_unzipped = zipfile.ZipFile(zip_file, 'r')
            file_unzipped.setpassword(str.encode(self._sec.getZip()))
            file_unzipped.extractall(self.raw_file_path)
            file_unzipped.close()
        except:
            raise Exception

    def raw_file_rename(self):

        file_list = os.listdir(self.raw_file_path)
        file_count = 0
        for file in file_list:
            if file in self.file_rename.keys():
                os.rename(os.path.join(self.raw_file_path, file),
                          os.path.join(self.raw_file_path, self.file_rename[file]))
                file_count += 1
            elif file in self.file_rename.values():
                file_count += 1

        return file_count

    def unzip_rename(self, zip_file):
        self.unzip_files(zip_file)
        file_count = self.raw_file_rename()
        if file_count == 13:
            return 'Unzipped and renamed'
        else:
            raise 'Not all files were unpacked and renamed'
