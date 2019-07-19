
# these defintions help with inserting only certain columns into
# a database table

def get_tbl_columns_query(connection, table):
    tbl_columns = connection.query('describe {};'.format(table))
    tbl_columns = [i['Field'] for i in tbl_columns]
    return tbl_columns


def get_shared_columns(export_cols, import_cols):
    shared_cols = [i for i in export_cols if i in import_cols]
    return shared_cols


def field_str_output(columns, prefix=''):
    fields = []
    for col in columns:
        fields.append(prefix+col)
    field_str = ", ".join(fields)
    return field_str


def var_str_output(columns):
    var_str = []
    for col in columns:
        var_str.append('%s')
    var_str = ", ".join(var_str)
    return var_str


def insert_sql_generator(columns, import_tbl, export_tbl=None, insert_ignore=False):

    if insert_ignore == False:
        insert_ignore = ''
    elif insert_ignore == True:
        insert_ignore = 'IGNORE'
    else:
        raise ValueError

    insert_field_str = field_str_output(columns)

    if export_tbl is None:
        var_str = var_str_output(columns)
        insert_sql_str = '''insert {} into {}
        ({}) values ({})'''.format(insert_ignore, import_tbl, insert_field_str, var_str)

    else:
        export_field_str = field_str_output(columns, 'et.')
        insert_sql_str = '''insert {} into {} ({})
        select {} from {} et'''.format(insert_ignore, import_tbl,
                                       insert_field_str, export_field_str, export_tbl)

    return insert_sql_str
