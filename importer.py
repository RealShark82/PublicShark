def pharsefilename():
    import sys
    global file_name
    file_name = sys.argv[1]
    print file_name
    if 'tempfile' in file_name:
        print 'tempfile'
        if '.csv' in file_name:
            print 'csv'
            connectdb()
            importCSV()
        elif '.xls' in file_name:
            print 'xls'
            connectdb()
            importxlrd()
        else:
            print 'Estensione non accettata'
            raise ValueError('Estensione non accettata')
    else:
        print 'Nome File errato'
        raise ValueError('Nome File non accettato')



def connectdb():
    import MySQLdb
    # Settings to connect to db MySQL
    global database
    database = MySQLdb.connect(host="localhost", user="root", passwd="", db="prova")

    # istruzione di scrittura nel db
    global cursor
    cursor = database.cursor()

    # Create the INSERT query
    global query
    query = "INSERT INTO temphist (tas, Year, Month, Country_id, DATA) VALUES (%s, %s, %s, %s, %s)"


def importxlrd():
    import xlrd
    # Open xls's document
    docexcel = xlrd.open_workbook(file_name)
    sheet = docexcel.sheet_by_name("tas_1901_2015")
    for r in range(1, sheet.nrows):
        tas = sheet.cell(r, 0).value
        Year = sheet.cell(r, 1).value
        Month = sheet.cell(r, 2).value
        Country_id = sheet.cell(r, 3).value
        if Country_id == "ITA":
            Country_id = "80"
        elif Country_id == "USA":
            Country_id = "168"

            # Read items in the xls sheet
        else:
            Country_id = "179"
        DATA = ('{}-{:g}-01'.format(Year, Month))
        values = (tas, Year, Month, Country_id, DATA)
        print values
        cursor.execute(query, values)
    closedb()


def importCSV():
    import csv
    # Open csv's document
    with open(file_name) as csvfile:
        readcsv = csv.reader(csvfile, delimiter=',')
        next(readcsv, None)
        for row in readcsv:
            tas = row[0]
            Year = row[1]
            Month = row[2]
            Country_id = row[3]
            ISO3 = row[4]
            ISO2 = row[5]
            if Country_id == "ITA":
                Country_id = "80"
            elif Country_id == "USA":
                Country_id = "168"

                # Read items in the xls sheet
            else:
                Country_id = "179"
            DATA = ('{}-{}-01'.format(Year, Month))
            values = (tas, Year, Month, Country_id, DATA)
            print values
            cursor.execute(query, values)
    closedb()


def closedb():
    cursor.close()
    # Write change in the db
    database.commit()
    # Close db connection
    database.close()
    print 'importazione terminata'
    raise ValueError('importazione terminata')


if __name__ == '__main__':
    pharsefilename()
