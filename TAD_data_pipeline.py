#!/usr/bin/env python
# coding: utf-8
#-------------------------------------------------------------------------------
# Name:        TAD_Data_Pipeline
# Purpose:     Extract, transform and load data from TAD property data to a database
#
# Author:      Imtiaz Syed
#
# Created:     04/20/2021
#-------------------------------------------------------------------------------


import arcpy
import pandas as pd
import os
from arcpy import env
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))


pd.set_option('display.max_columns', None)
pd.set_option('precision', 0)

# add the property data text file
df = pd.read_csv(r"E:\Python_GIS_Bigdata\Data\PropertyData.txt", sep="|", encoding = 'ISO-8859-1', dtype='object')
df.columns = df.columns.str.strip()

# define an empty list to uncode the CityNames values
CityNames = []

for city in df.loc[:,'City']:
    if city == '000':
        CityNames.append('Null')
    elif city == '001':
        CityNames.append('Azle')
    elif city == '002':
        CityNames.append('Bedford')
    elif city == '003':
        CityNames.append('Benbrook')
    elif city == '004':
        CityNames.append('Blue Mound')
    elif city == '005':
        CityNames.append('Colleyville')
    elif city == '006':
        CityNames.append('Crowley')
    elif city == '007':
        CityNames.append('Dalworthington Gardens')
    elif city == '008':
        CityNames.append('Edgecliff Village')
    elif city == '009':
        CityNames.append('Everman')
    elif city == '010':
        CityNames.append('Forest Hill')
    elif city == '011':
        CityNames.append('Grapevine')
    elif city == '012':
        CityNames.append('Null')
    elif city == '013':
        CityNames.append('Keller')
    elif city == '014':
        CityNames.append('Kennedale')
    elif city == '015':
        CityNames.append('Lakeside')
    elif city == '016':
        CityNames.append('Lake Worth')
    elif city == '017':
        CityNames.append('Mansfield')
    elif city == '018':
        CityNames.append('North Richland Hills')
    elif city == '019':
        CityNames.append('Pantego')
    elif city == '020':
        CityNames.append('Richland Hills')
    elif city == '021':
        CityNames.append('Saginaw')
    elif city == '022':
        CityNames.append('Southlake')
    elif city == '023':
        CityNames.append('Westover Hills')
    elif city == '024':
        CityNames.append('Arlington')
    elif city == '025':
        CityNames.append('Euless')
    elif city == '026':
        CityNames.append('Fort Worth')
    elif city == '027':
        CityNames.append('Haltom City')
    elif city == '028':
        CityNames.append('Hurst')
    elif city == '029':
        CityNames.append('River Oaks')
    elif city == '030':
        CityNames.append('White Settlement')
    elif city == '031':
        CityNames.append('Watauga')
    elif city == '032':
        CityNames.append('Westworth Village')
    elif city == '033':
        CityNames.append('Burleson')
    elif city == '034':
        CityNames.append('Haslet')
    elif city == '035':
        CityNames.append('Briar')
    elif city == '036':
        CityNames.append('Pelican Bay')
    elif city == '037':
        CityNames.append('Westlake')
    elif city == '038':
        CityNames.append('Grand Prairie')
    elif city == '039':
        CityNames.append('Sansom Park')
    elif city == '040':
        CityNames.append('Newark')
    elif city == '041':
        CityNames.append('Reno')
    elif city == '042':
        CityNames.append('Flower Mound')
    elif city == '043':
        CityNames.append('Roanoke')
    elif city == '044':
        CityNames.append('Trophy Club')
    else:
        CityNames.append('Null')

df['CityNames'] = pd.Series(CityNames)

# define an empty list to uncode the ISD values
ISD = []

for SC in df.loc[:,'School']:
    if SC == '000':
        ISD.append('Null')
    elif SC == '901':
        ISD.append('Arlington')
    elif SC == '902':
        ISD.append('Birdville')
    elif SC == '904':
        ISD.append('Everman')
    elif SC == '905':
        ISD.append('Fort Worth')
    elif SC == '906':
        ISD.append('Grapevine-Colleyville')
    elif SC == '907':
        ISD.append('Keller')
    elif SC == '908':
        ISD.append('Mansfield')
    elif SC == '910':
        ISD.append('Lake Worth')
    elif SC == '911':
        ISD.append('Northwest')
    elif SC == '912':
        ISD.append('Crowley')
    elif SC == '914':
        ISD.append('Kennedale')
    elif SC == '915':
        ISD.append('Azle')
    elif SC == '916':
        ISD.append('Hurst-Euless-Bedford')
    elif SC == '917':
        ISD.append('Castleberry')
    elif SC == '918':
        ISD.append('Eagle Mountain-Saginaw')
    elif SC == '919':
        ISD.append('Carroll')
    elif SC == '920':
        ISD.append('White Settlement')
    elif SC == '921':
        ISD.append('Aledo')
    elif SC == '922':
        ISD.append('Burleson')
    elif SC == '923':
        ISD.append('Godley')
    elif SC == '924':
        ISD.append('Lewisville')
    else:
        ISD.append('Null')

df['ISD'] = pd.Series(ISD)


# define an empty property tax list to store the property tax
propertytax_list = []

for index, row in df.loc[:, 'Total_Value'].iteritems():
    propertytax = (float(row) * 2.10)/100
    propertytax_list.append(propertytax)

df['Property_Tax'] = pd.Series(propertytax_list)

# defint the owner zipcode list to calculate the zipcode values
Owner_ZipCode = []

for Owner_Zip, Owner_Zip4 in zip(df.Owner_Zip, df.Owner_Zip4):
    if Owner_Zip4.isspace() and not Owner_Zip.isspace():
        Owner_ZipCode.append(Owner_Zip)
    elif Owner_Zip.isspace() and Owner_Zip4.isspace():
        Owner_ZipCode.append('None')
    else:
        Owner_ZipCode.append(Owner_Zip + '-' + Owner_Zip4)

df['Owner_ZipCode'] = pd.Series(Owner_ZipCode)

df['TAXPIN'] = df['PIDN']
df['TaxpinAccount'] = df['PIDN'] + '.' + df['Account_Num']
df['County'] = '220'
df['Countyname'] = 'Tarrant'
df['Appraisal_Year'] = 2021
df['MAPSCO'] = df['MAPSCO'].str.rstrip()
    
# define the individual address components empty lists

Street_Type = []
Street_Name = []
Street_Address = []
Add_No = []
Prefix = []
Suffix = []

# dirctions lookup list
Directions = ['N','E','W','S', 'North', 'East', 'West', 'South', 'NE', 'NW', 'SE', 'SW', 'NORTH', 'EAST', 'WEST', 'SOUTH']

# split the Situs_Address column into individual columns and expand the columns
Situs_Address = df['Situs_Address'].str.split(expand = True)

for row in Situs_Address.loc[:].values.tolist():
        
        ############################
        # 7 worded address use-cases
        ############################
        
        # 7 worded addresses and row[0] is numeric and row[1] is directional and row[6] is equal to 'ST'

        if row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[6] == 'ST':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append('ST')
            
        
        # 7 worded addresses and row[0] is numeric and row[1] is directional and row[6] is not equal to 'ST' and row[4] is equal to '1187'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[6] != 'ST' and row[4] == '1187':


            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3])
            Street_Type.append(row[3])
            
            
        # 7 worded addresses and row[0] is numeric and row[1] is directional and row[6] is not equal to 'ST' and row[4] is not equal to '1187'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[6] != 'ST' and row[4] != '1187':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2] + ' ' + row[3])
            Street_Address.append(row[2] + ' ' + row[3])
            Street_Type.append(row[4])
        
        # 7 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is equal to ('RD' or 'CT' or 'AVE')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] == 'RD' or row[4] == 'CT' or row[4] == 'AVE'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[4])
        
        # 7 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('RD' or 'CT' or 'AVE') and row[2] is equal to 'PL'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != 'RD' or row[4] != 'CT' or row[4] != 'AVE') and row[2] == 'PL':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2])
            Street_Type.append(row[2])

        # 7 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('RD' or 'CT' or 'AVE') and row[2] is not equal to 'PL' and length of row[3] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != 'RD' or row[4] != 'CT' or row[4] != 'AVE') and row[2] != 'PL' and len(row[3]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[4])
        
        # 7 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('RD' or 'CT' or 'AVE') and row[2] is not equal to 'PL' and length of row[3] is less than or equal to 5 and length of row[4] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != 'RD' or row[4] != 'CT' or row[4] != 'AVE') and row[2] != 'PL' and len(row[3]) <= 5 and len(row[4]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5] + ' ' + row[6])
            Street_Type.append(row[6])

        # 7 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('RD' or 'CT' or 'AVE') and row[2] is not equal to 'PL' and length of row[3] is less than or equal to 5 and length of row[4] is less than or equal to 5    
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and row[6] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != 'RD' or row[4] != 'CT' or row[4] != 'AVE') and row[2] != 'PL' and len(row[3]) <= 5 and len(row[4]) <= 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[4])
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[3])


        ############################
        # 6 worded address use-cases
        ############################
        
        # 6 worded addresses and row[0] is numeric and row[1] is directional and row[5] is equal to 'RD'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[5] == 'RD':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[5])

        # 6 worded addresses and row[0] is numeric and row[1] is directional and row[5] is not equal to 'RD' and length of row[3] is greater than 5 and row[3] is equal to 'SOUTHWEST'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[5] != 'RD' and len(row[3]) > 5 and row[3] == 'SOUTHWEST':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append('ST')
        
        # 6 worded addresses and row[0] is numeric and row[1] is directional and row[5] is not equal to 'RD' and length of row[3] is greater than 5 and row[3] is not equal to 'SOUTHWEST' and row[3] is equal to 'WRIGHT'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[5] != 'RD' and len(row[3]) > 5 and row[3] != 'SOUTHWEST' and row[3] == 'WRIGHT':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append('FWY')

        # 6 worded addresses and row[0] is numeric and row[1] is directional and row[5] is not equal to 'RD' and length of row[3] is greater than 5 and row[3] is not equal to 'SOUTHWEST' and row[3] is not equal to 'WRIGHT'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[5] != 'RD' and len(row[3]) > 5 and row[3] != 'SOUTHWEST' and row[3] != 'WRIGHT':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append('RD')

        # 6 worded addresses and row[0] is numeric and row[1] is directional and row[5] is not equal to 'RD' and length of row[3] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[5] != 'RD' and len(row[3]) <= 5:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is equal to ('#' or 'UNIT') and row[1] is equal to 'ASCENSION'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] == '#' or row[4] == 'UNIT') and row[1] == 'ASCENSION':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is equal to ('#' or 'UNIT') and row[1] is equal to ('HARLANWOOD' or 'ENSIGN' or 'BELLAIRE')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] == '#' or row[4] == 'UNIT') and (row[1] == 'HARLANWOOD' or row[1] == 'ENSIGN' or row[1] == 'BELLAIRE'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[2])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is equal to ('#' or 'UNIT') and row[1] is equal to ('FM' or 'STATE')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] == '#' or row[4] == 'UNIT') and (row[1] == 'FM' or row[1] == 'STATE'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[2])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[1] is equal to ('GOLDEN' or 'SYCAMORE' or 'PARK' or 'VICTORIA' or 'ST' or 'EDWARDS' or'ALTA' or'TURNER' or'BOWMAN' or 'DAN')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[1] == 'GOLDEN' or row[1] == 'SYCAMORE' or row[1] == 'PARK' or row[1] == 'VICTORIA' or row[1] == 'ST'or row[1] == 'EDWARDS' or row[1] == 'ALTA' or row[1] == 'TURNER' or row[1] == 'BOWMAN' or row[1] == 'DAN'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[1] is equal to ('PECAN' or 'COUNT' or 'SILVER' or 'BAIRD' or 'FOREST' or 'CAMELLIA' or 'PARKER' or 'RANDOL' or 'HOLLY' or 'MONTE' or 'MARTIN' or 'BOAT' or 'BEAR' or 'RANDY' or 'COPPER' or 'QUAIL' or 'BOCA')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[1] == 'PECAN' or row[1] == 'COUNT' or row[1] == 'SILVER' or row[1] == 'BAIRD' or row[1] == 'FOREST' or row[1] == 'CAMELLIA' or row[1] == 'PARKER' or row[1] == 'RANDOL' or row[1] == 'HOLLY' or row[1] == 'MONTE' or row[1] == 'MARTIN' or row[1] == 'BOAT' or row[1] == 'BEAR' or row[1] == 'RANDY' or row[1] == 'COPPER' or row[1] == 'QUAIL' or row[1] == 'BOCA'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[1] is equal to ('CLIFFORD' or 'PRECINCT' or 'CAMP' or 'SIX' or 'LANDS' or 'HERITAGE' or 'BRYANT' or 'LANDS' or 'RUFE' or 'GREEN' or 'WESTERN' or 'TECH' or 'HEATHER' or 'GRAPEVINE' or 'MINTERS' or 'MERCANTILE' or 'RAY')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[1] == 'CLIFFORD' or row[1] == 'PRECINCT' or row[1] == 'CAMP' or row[1] == 'SIX' or row[1] == 'LANDS' or row[1] == 'HERITAGE' or row[1] == 'BRYANT' or row[1] == 'LANDS' or row[1] == 'RUFE' or row[1] == 'GREEN' or row[1] == 'WESTERN' or row[1] == 'TECH' or row[1] == 'HEATHER' or row[1] == 'GRAPEVINE' or row[1] == 'MINTERS' or row[1] == 'MERCANTILE' or row[1] == 'RAY'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is equal to ('BLVD' or 'DR' or 'RD')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] == 'BLVD' or row[4] == 'DR' or row[4] == 'RD'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[4])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is equal to ('APT' or 'LOT')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] == 'APT' or row[4] == 'LOT'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is not equal to ('APT' or 'LOT') and row[5] is equal to ('PKWY' or 'RD' or 'TR')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'APT' or row[4] != 'LOT') and (row[5] == 'PKWY' or row[5] == 'RD' or row[5] == 'TR'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[5])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4]  is not equal to ('BLVD' or 'DR' or 'RD') and row[3] is equal to '#' 
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'BLVD' or row[4] != 'DR' or row[4] != 'RD') and row[3] == '#':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[2])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is not equal to ('BLVD' or 'DR' or 'RD') and row[3] is not equal to ('#' or 'RD' or 'HWY')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'BLVD' or row[4] != 'DR' or row[4] != 'RD') and row[3] != '#' and (row[2] == 'RD' or row[2] == 'HWY'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[2])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is not equal to ('BLVD' or 'DR' or 'RD') and row[3] is not equal to ('#' or 'RD' or 'HWY') and length of row[3] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'BLVD' or row[4] != 'DR' or row[4] != 'RD') and row[3] != '#' and (row[2] != 'RD' or row[2] != 'HWY') and len(row[3]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(None)
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is not equal to ('BLVD' or 'DR' or 'RD') and row[3] is not equal to ('#' or 'RD' or 'HWY') and length of row[3] is less than or equal to 5 and row[3] is directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'BLVD' or row[4] != 'DR' or row[4] != 'RD') and row[3] != '#' and (row[2] != 'RD' or row[2] != 'HWY') and len(row[3]) <= 5 and row[3] in Directions:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[2])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is not directional and row[4] is not equal to ('BLVD' or 'DR' or 'RD') and row[3] is not equal to ('#' or 'RD' or 'HWY') and length of row[3] is less than or equal to 5 and row[3] is not directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] not in Directions and (row[4] != 'BLVD' or row[4] != 'DR' or row[4] != 'RD') and row[3] != '#' and (row[2] != 'RD' or row[2] != 'HWY') and len(row[3]) <= 5 and row[3] not in Directions:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4] + ' ' + row[5])
            Street_Type.append(row[3])

        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is directional and row[1] is not equal to 'STONE'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] in Directions and row[1] != 'STONE':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[5])
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[4])
        
        # 6 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not equal to ('#' or 'UNIT') and row[5] is directional and row[1] is equal to STONE
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and (row[4] != '#' or row[4] != 'UNIT') and row[5] in Directions and row[1] == 'STONE':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[5])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[3])
            
        # 6 worded addresses and row[0] is not numeric and row[1] is equal to 'BUS'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and row[5] is not None and str(row[0]).isnumeric() == False and row[1] == 'BUS':

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[5])


        ############################
        # 5 worded address use-cases
        ############################
        
        # 5 worded addresses and row[0] is numeric and row[1] is directional and row[4] is directional and row[2] is equal to 'LOOP'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[4] in Directions and row[2] == 'LOOP':

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(row[4])
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3])
            Street_Type.append('HWY')

        # 5 worded addresses and row[0] is numeric and row[1] is directional and row[4] is directional and row[2] is not equal to 'LOOP'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[4] in Directions and row[2] != 'LOOP':

            Add_No.append(row[4])
            Prefix.append(row[1])
            Suffix.append(row[4])
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3])
            Street_Type.append(row[3])

        # 5 worded addresses and row[0] is numeric and row[1] is directional and row[4] is not directional and row[4] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[4] not in Directions and str(row[4]).isnumeric() == True:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[3])
        
        # 5 worded addresses and row[0] is numeric and row[1] is directional and row[4] is not directional and row[4] is not numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2] + ' ' + row[3])
            Street_Address.append(row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[3])

        # 5 worded addresses and row[0] is numeric and row[1] is directional and row[4] is directional and row[3] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] in Directions and str(row[3]).isnumeric() == True:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[4])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])
        
         # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is directional and row[3] is not numeric and length of row[3] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] in Directions and str(row[3]).isnumeric() == False and len(row[3]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[4])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append('RD')
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is directional and row[3] is not numeric and length of row[3] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] in Directions and str(row[3]).isnumeric() == False and len(row[3]) <= 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[4])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[3])
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is numeric and row[3] is equal to 'HWY'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == True and row[3] == 'HWY':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[3])
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is numeric and row[3] is not equal to 'HWY' and length of row[2] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == True and row[3] != 'HWY' and len(row[2]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append('RD')
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is numeric and row[3] is not equal to 'HWY' and length of row[2] is less than or equal to 5 and row[1] is equal to 'HWY'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == True and row[3] != 'HWY' and len(row[2]) <= 5 and row[1] == 'HWY':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append('HWY')
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is numeric and row[3] is not equal to 'HWY' and length of row[2] is less than or equal to 5 and row[1] is equal to 'HWY'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == True and row[3] != 'HWY' and len(row[2]) <= 5 and row[1] == 'HWY':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[2])

        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is equal to '#' and row[2] is equal to 'STATION'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] == '#' and row[2] == 'STATION':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(None)

        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is equal to '#' and row[2] is not equal to 'STATION'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] == '#' and row[2] != 'STATION':

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[2])
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is not equal to '#' and ('#' or '-') in row[4] and row[2] is equal to ('RD' or 'ST')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] != '#' and ('#' in row[4] or '-' in row[4]) and (row[2] == 'RD' or row[2] == 'ST'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[2])
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is not equal to '#' and ('#' or '-') in row[4] and row[2] is not equal to ('RD' or 'ST')
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] != '#' and ('#' in row[4] or '-' in row[4]) and (row[2] != 'RD' or row[2] != 'ST'):

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[3])

        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is not equal to '#' and ('#' or '-') not in row[4] and length of row[4] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] != '#' and ('#' not in row[4] or '-' not in row[4]) and len(row[4]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[2])
        
        # 5 worded addresses and row[0] is numeric and row[1] is not directional and row[4] is not directional and row[4] is not numeric and row[3] is not equal to '#' and ('#' or '-') not in row[4] and length of row[4] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and row[4] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and row[4] not in Directions and str(row[4]).isnumeric() == False and row[3] != '#' and ('#' not in row[4] or '-' not in row[4]) and len(row[4]) <= 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3] + ' ' + row[4])
            Street_Type.append(row[4])


        ############################
        # 4 worded address use-cases
        ############################
        
        # 4 worded addresses and row[0] is numeric and row[1] is directional and row[3] is numeric and length of row[2] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and str(row[3]).isnumeric() == True and len(row[2]) > 5:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append('HWY')

        # 4 worded addresses and row[0] is numeric and row[1] is directional and row[3] is numeric and length of row[2] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and str(row[3]).isnumeric() == True and len(row[2]) <= 5:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])

        # 4 worded addresses and row[0] is numeric and row[1] is directional and row[3] is not numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] in Directions and str(row[3]).isnumeric() == False:

            Add_No.append(row[0])
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[2])
            Street_Address.append(row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[3])
            
        # 4 worded addresses and row[0] is numeric and row[1] is not directional and row[3] is numeric and length of row[2] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and str(row[3]).isnumeric() == True and len(row[2]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append('HWY')
            
        # 4 worded addresses and row[0] is numeric and row[1] is not directional and row[3] is numeric and length of row[2] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and str(row[3]).isnumeric() == True and len(row[2]) <= 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])

        # 4 worded addresses and row[0] is numeric and row[1] is not directional and row[3] is not numeric and length of row[3] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and str(row[3]).isnumeric() == False and len(row[3]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(None)

        # 4 worded addresses and row[0] is numeric and row[1] is not directional and row[3] is not numeric and length of row[3] is less than or equal to 5 and row[3] is directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and str(row[3]).isnumeric() == False and len(row[3]) <= 5 and row[3] in Directions:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])
            
        # 4 worded addresses and row[0] is numeric and row[1] is not directional and row[3] is not numeric and length of row[3] is less than or equal to 5 and row[3] is not directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == True and row[1] not in Directions and str(row[3]).isnumeric() == False and len(row[3]) <= 5 and row[3] not in Directions:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[3])

        # 4 worded addresses and row[0] is not numeric and row[0] is directional and row[3] is directional and row[2] is not numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[3] in Directions and str(row[2]).isnumeric() == False:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(row[3])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])

        # 4 worded addresses and row[0] is not numeric and row[0] is directional and row[3] is directional and row[2] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[3] in Directions and str(row[2]).isnumeric() == True:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(row[3])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[1])
            
        # 4 worded addresses and row[0] is not numeric and row[0] is not directional and row[3] is directional and row[2] is equal to 'RD'
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[3] in Directions and row[2] == 'RD':

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(row[3])
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])

        # 4 worded address and row[0] is not numeric and row[0] is not directional and row[3] is directional and row[2] is not equal to 'Rd' and row[1] is equal to 'HWY' or 'DAY' and row[2] is not numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[3] in Directions and row[2] != 'Rd' and (row[1] == 'HWY' or row[1] == 'DAY') and str(row[2]).isnumeric() == False:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(None)
    

        # 4 worded address and row[0] is not numeric and row[0] is not directional and row[3] is directional and row[2] is not equal to 'Rd' and row[1] is not equal to 'HWY' or 'DAY' and row[2] is not numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[3] in Directions and row[2] != 'Rd' and (row[1] != 'HWY' or row[1] != 'DAY') and str(row[2]).isnumeric() == False:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])
        
        # 4 worded address and row[0] is not numeric and row[0] is not directional and row[3] is directional and row[2] is not equal to 'Rd' and row[1] is not equal to 'HWY' or 'DAY' and row[2] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[3] in Directions and row[2] != 'Rd' and (row[1] != 'HWY' or row[1] != 'DAY') and str(row[2]).isnumeric() == True:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(row[3])
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[1])
        
        # 4 worded address and row[0] is not numeric and row[3] is numeric and row[0] is directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and str(row[3]).isnumeric() == True and row[0] in Directions:

            Add_No.append(None)
            Prefix.append(row[1])
            Suffix.append(None)
            Street_Name.append(row[1] + ' ' + row[2])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(row[2])
        
        # 4 worded address and row[0] is not numeric and row[3] is numeric and row[0] is not directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and row[3] is not None and str(row[0]).isnumeric() == False and str(row[3]).isnumeric() == True and row[0] not in Directions:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2] + ' ' + row[3])
            Street_Type.append(None)

        
        ############################
        # 3 worded address use-cases
        ############################
    
        # 3 worded addresses where row[0] is not numeric and row[0] is directional and row[2] is directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[2] in Directions:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(row[2])
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[1])

        # 3 worded addresses where row[0] is not numeric and row[0] is directional and row[2] is not directional and row[2] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[2] not in Directions and str(row[2]).isnumeric() == True:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(None)
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[1])
            
        # 3 worded addresses where row[0] is not numeric and row[0] is directional and row[2] is not directional and row[2] is not numeric and length of row[2] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[2] not in Directions and str(row[2]).isnumeric() == False and len(row[2]) > 5:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[3])

        # 3 worded addresses where row[0] is not numeric and row[0] is directional and row[2] is not directional and row[2] is not numeric and length of row[2] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] in Directions and row[2] not in Directions and str(row[2]).isnumeric() == False and len(row[2]) <= 5:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[2])

        # 3 worded addresses where row[0] is not numeric and row[0] is not directional and row[2] is directional and (row[0] is equal to 'HWY' or 'IH' or 'LOOP')
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[2] in Directions and (row[0] == 'HWY' or row[0] == 'IH' or row[0] == 'LOOP'):

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(row[2])
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[0])

        # 3 worded addresses where row[0] is not numeric and row[0] is not directional and row[2] is directional and (row[0] is not equal to 'HWY' or 'IH' or 'LOOP')
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[2] in Directions and (row[0] != 'HWY' or row[0] != 'IH' or row[0] != 'LOOP'):

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(row[2])
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[1])
        
        # 3 worded addresses where row[0] is not numeric and row[0] is not directional and row[2] is not directional and row[2] is numeric
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and row[2] not in Directions and str(row[2]).isnumeric() == True:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[0] + ' ' + row[1])
            Street_Address.append(row[0] + ' ' + row[1] + ' ' + row[2])
            Street_Type.append(row[1])
        
        # 3 worded addresses where row[0] is numeric and row[2] is directional
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == True and row[2] in Directions:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(row[2])
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2])
            Street_Type.append('RD')

        # 3 worded addresses where row[0] is numeric and row[2] is not directional and length of row[2] is greater than 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == True and row[2] not in Directions and len(row[2]) > 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2])
            Street_Type.append(None)

        # 3 worded addresses where row[0] is numeric and row[2] is not directional and length of row[2] is less than or equal to 5
        elif row[0] is not None and row[1] is not None and row[2] is not None and str(row[0]).isnumeric() == True and row[2] not in Directions and len(row[2]) <= 5:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1] + ' ' + row[2])
            Street_Type.append(row[2])
            

        ############################
        # 2 worded address use-cases
        ############################
        
        
        # 2 worded addresses where row[0] is numeric
        elif row[0] is not None and row[1] is not None and str(row[0]).isnumeric() == True:

            Add_No.append(row[0])
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(row[1])
            Street_Address.append(row[1])
            Street_Type.append(None)
        
        # 2 worded addresses where row[0] is not numeric and row[0] is directional
        elif row[0] is not None and row[1] is not None and str(row[0]).isnumeric() == False and row[0] in Directions:

            Add_No.append(None)
            Prefix.append(row[0])
            Suffix.append(None)
            Street_Name.append(None)
            Street_Address.append(row[0] + ' ' + row[1])
            Street_Type.append('ST')
        
        # 2 worded addresses where row[0] is not numeric and row[0] is not directional and row[1] is numeric and length of row[0] is greater than 5
        elif row[0] is not None and row[1] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and str(row[1]).isnumeric() == True and len(row[0]) > 5:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(None)
            Street_Address.append(row[0] + ' ' + row[1])
            Street_Type.append('BLVD')
            
        # 2 worded addresses where row[0] is not numeric and row[0] is not directional and row[1] is numeric and length of row[0] less than or equal to 5
        elif row[0] is not None and row[1] is not None and str(row[0]).isnumeric() == False and row[0] not in Directions and str(row[1]).isnumeric() == True and len(row[0]) <= 5:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(None)
            Street_Address.append(row[0] + ' ' + row[1])
            Street_Type.append(row[0])


        else:

            Add_No.append(None)
            Prefix.append(None)
            Suffix.append(None)
            Street_Name.append(None)
            Street_Address.append(None)
            Street_Type.append(None)

df['Street_No'] = pd.Series(Add_No)
df['Prefix'] = pd.Series(Prefix)
df['Street_Type'] = pd.Series(Street_Type)
df['Street_Name'] = pd.Series(Street_Name)
df['Street_Address'] = pd.Series(Street_Address)
df['Suffix'] = pd.Series(Suffix)
df['Prefix'] = df['Prefix'].str.rstrip()
df['Street_Type'] = df['Street_Type'].str.rstrip()
    

try:

    env.overwriteOutput = True
    env.workspace = r'C:\Users\91970\Documents\ArcGIS\Projects\BootcampGIS\BootcampGIS.gdb'
    fcname = 'PropertyData'
    fclass = os.path.join(arcpy.env.workspace,fcname)

    # fields from the PropertyData table
    fields = ['RP','Appraisal_Year','Account_Num','Record_Type','Sequence_No','PIDN',
              'Owner_Name','Owner_Address','Owner_CityState','Owner_Zip','Owner_Zip4',
              'Owner_CRRT','Situs_Address','Property_Class','TAD_Map','MAPSCO','Exemption_Code',
              'State_Use_Code','LegalDescription','Notice_Date','County','City','School',
              'Num_Special_Dist','Spec1','Spec2','Spec3','Spec4','Spec5','Deed_Date','Deed_Book',
              'Deed_Page','Land_Value','Improvement_Value','Total_Value','Garage_Capacity',
              'Num_Bedrooms','Num_Bathrooms','Year_Built','Living_Area','Swimming_Pool_Ind',
              'ARB_Indicator','Ag_Code','Land_Acres','Land_SqFt','Ag_Acres','Ag_Value',
              'Central_Heat_Ind','Central_Air_Ind','Structure_Count','From_Accts','Appraisal_Date',
              'Appraised_Value','GIS_Link','Instrument_No','Overlap_Flag','PropertyTax',
              'CityName', 'CountyName', 'Street_No', 'Street_Name', 'Street_Address', 'Street_Type', 'Street_Type', 'Prefix', 'Suffix']

    
    # fields from the pandas dataframe
    inputs = df.loc[:,['RP','Appraisal_Year','Account_Num','Record_Type','Sequence_No','PIDN',
              'Owner_Name','Owner_Address','Owner_CityState','Owner_Zip','Owner_Zip4',
              'Owner_CRRT','Situs_Address','Property_Class','TAD_Map','MAPSCO','Exemption_Code',
              'State_Use_Code','LegalDescription','Notice_Date','County','City','School',
              'Num_Special_Dist','Spec1','Spec2','Spec3','Spec4','Spec5','Deed_Date','Deed_Book',
              'Deed_Page','Land_Value','Improvement_Value','Total_Value','Garage_Capacity',
              'Num_Bedrooms','Num_Bathrooms','Year_Built','Living_Area','Swimming_Pool_Ind',
              'ARB_Indicator','Ag_Code','Land_Acres','Land_SqFt','Ag_Acres','Ag_Value',
              'Central_Heat_Ind','Central_Air_Ind','Structure_Count','From_Accts','Appraisal_Date',
              'Appraised_Value','GIS_Link','Instrument_No','Overlap_Flag','Property_Tax',
              'CityNames', 'Countyname', 'Street_No', 'Street_Name', 'Street_Address', 'Street_Type', 'Street_Type', 'Prefix', 'Suffix']].values.tolist()
    
    # create an InsertCursor object to insert rows
    InsertCursor = arcpy.da.InsertCursor(fclass,fields)

    for row in inputs:
        InsertCursor.insertRow(row)

    del InsertCursor

except Exception as ex:
    print(str(ex))