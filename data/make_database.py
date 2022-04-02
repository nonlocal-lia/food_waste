import tabula
import pandas as pd
import sqlite3
from os.path import exists

def clean_pdf_data(df, metal_label_row=None, compost=False, col_label_data=False, split_row=False, other_wastes_label=False):
    if split_row:
        df.drop([df.shape[0]-3], inplace=True)
        df.replace('Wastes', 'Miscellaneous Inorganic Wastes', inplace=True)

    # removing metal and other wastes rows that were just labels in table and relabing data for clarity
    if metal_label_row:
        for n in range(1,4):
            df.iloc[metal_label_row  +n, 0] = df.iloc[metal_label_row, 0] +" - "+df.iloc[metal_label_row + n, 0]
        df.drop([metal_label_row], inplace=True)
    
    if other_wastes_label:
        df.drop([13], inplace=True)
    
    if compost:
        df.drop([1,3], inplace=True)

    # if column labels are actual data, copying data into table
    if col_label_data:
        df = df.columns.to_frame().T.append(df, ignore_index=True)
    
    # renaming columns and setting index to row labels
    df.rename(columns={df.columns[0]: 'Material'}, inplace=True)
    df.set_index('Material', inplace=True)

    # Filling missing values
    df.replace('Neg.', 0, inplace=True)
    df.fillna('null', inplace=True)
    return df

def merge_split_table(df1, df2):
    # Fixing labels that interpreter treated as seperate lines
    df1.iloc[0,0] = df1.iloc[0,0] + ' ' + df1.iloc[1,0]
    df1.iloc[2,0] = df1.iloc[2,0] + ' ' + df1.iloc[3,0]
    df1.iloc[6,0] = df1.iloc[6,0] + ' ' + df1.iloc[7,0]

    # droping extra lines
    df1.drop([1,3,7] ,inplace=True)

    # resetting index
    df1.reset_index(inplace=True)
    df1.drop(columns=['index'], inplace=True)

    # column title is actually a value, creating row from column title
    df1 = df1.columns.to_frame().T.append(df1, ignore_index=True)
    
    # column title is actually a value, creating row from column title
    df2 = df2.columns.to_frame().T.append(df2, ignore_index=True)

    # Joining and relabeling columns
    output = pd.concat([df1, df2], axis=1)
    output.columns = ['Material', '2018']
    return output

def populate_disposal_table(cursor):
    disposal_types = ["combustion",
                      "composting",
                      "recycling",
                      "landfill",
                      "animal feed",
                      "bio-based materials/biochemical processing",
                      "codigestion/anaerobic digestion",
                      "donation",
                      "land application",
                      "sewer/wastewater treatment"]
    for d in disposal_types:
        cursor.execute("""INSERT INTO disposal (disposal_type) 
                        VALUES ('{}');
                       """.format(d))
    return None

def populate_material_table(cursor):
    materials = ['Paper and Paperboard',
                 'Glass',
                 'Metals - Ferrous',
                 'Metals - Aluminum',
                 'Metals - Other Nonferrous',
                 'Plastics',
                 'Rubber and Leather',
                 'Textiles',
                 'Wood',
                 'Other',
                 'Food',
                 'Yard Trimmings',
                 'Miscellaneous Inorganic Wastes']
    products = materials[0:-3]
    metals = materials[2:5]

    for m in materials:
        product = 0
        material_type = m
        if m in products:
            product = 1
        if m in metals:
            material_type = 'Metals'
        cursor.execute("""INSERT INTO material (product, material_type, material_subtype) 
                    VALUES ('{}', '{}', '{}');
                    """.format(product, material_type, m))
    return None

def add_waste_category(df, years, cursor, recycle=False, compost=False, combust=False, landfill=False, oth=False):
    
    #Determining what rows to exclude
    if combust:
        drop_list = [5,11,15,16]
        disposal_id = 1
    if compost:
        drop_list = [3]
        disposal_id = 2
    if recycle:
        drop_list = [5,11]
        disposal_id = 3
    if landfill:
        drop_list = [5,11,15]
        disposal_id = 4
    if oth:
        drop_list = [6]
  
    if landfill or combust or recycle or compost:
        # Preparing df
        df.reset_index(inplace=True)
        df.drop(drop_list, inplace=True)
        df.reset_index(inplace=True)
        df.drop(columns=['index'], inplace=True)
        # Populating database
        for row in df.index:
            if compost:
                material_id = row+11
            else:
                material_id = row+1
            for i, year in enumerate(years):
                amount = df.iloc[row, i+1]
                if amount == 'null':
                    cursor.execute("""INSERT INTO waste (material_id, disposal_id, year, waste_in_tons) 
                                    VALUES ('{}', '{}', '{}', NULL);
                                """.format(material_id, disposal_id, year))
                else:
                    cursor.execute("""INSERT INTO waste (material_id, disposal_id, year, waste_in_tons) 
                                    VALUES ('{}', '{}', '{}', '{}');
                                """.format(material_id, disposal_id, year, amount))   

    if oth:
        # Preparing df
        df.drop(drop_list, inplace=True)
        df.reset_index(inplace=True)
        df.drop(columns=['index'], inplace=True)
        # Populating database
        material_id = 11
        for row in df.index:
            year = 2018
            amount = df.iloc[row, 1]
            disposal_id = row+5
            if amount == 'null':
                cursor.execute("""INSERT INTO waste (material_id, disposal_id, year, waste_in_tons) 
                                    VALUES ('{}', '{}', '{}', NULL);
                                """.format(material_id, disposal_id, year))
            else:
                cursor.execute("""INSERT INTO waste (material_id, disposal_id, year, waste_in_tons) 
                                    VALUES ('{}', '{}', '{}', '{}');
                                """.format(material_id, disposal_id, year, amount))
    return None

def populate_waste_table(df_list, years, cursor):
    for df in df_list:
        (landfill, combust, recycle, compost, oth) = (False, False, False, False, False)
        if df.shape[0] == other.shape[0]:
            oth=True
            print("Other OK")
        elif df.shape[0] == landfilled.shape[0]:
            landfill = True
            print("Landfill OK")
        elif df.shape[0] == combusted.shape[0]:
            combust = True
            print("Combust OK")
        elif df.shape[0] == composted.shape[0]:
            compost = True
            print("Compost OK")
        elif df.shape[0] == recycled.shape[0]:
            recycle = True
            print("Recycle OK")
        else:
            print(df)
            print(df.shape[0])
            raise ValueError('Dataframe is unknown or improperly formatted')
        add_waste_category(df,
                           years,
                           cursor,
                           recycle=recycle,
                           compost=compost,
                           combust=combust,
                           landfill=landfill,
                           oth=oth)
        

if __name__ == '__main__':
    if exists('./wasted_data.sqlite'):
        print('Database already exists')

    else:
        # Gathering Data from PDF
        pdf = "./2018_tables_and_figures_dec_2020_fnl_508.pdf"
        template = "./tabula-2018_tables_and_figures_dec_2020_fnl_508.json"
        table = tabula.read_pdf_with_template(input_path=pdf, template_path=template)
        recycled = table[2]
        composted = table[3]
        other_label = table[4]
        other_values = table[5]
        combusted = table[10]
        landfilled = table[12]

        print(landfilled)

        # Cleaning anomolies from tabula
        recycled = clean_pdf_data(recycled, metal_label_row=1, col_label_data=True)
        composted = clean_pdf_data(composted, compost=True)
        combusted = clean_pdf_data(combusted, metal_label_row=2, other_wastes_label=True)
        landfilled = clean_pdf_data(landfilled, metal_label_row=2, other_wastes_label=True, split_row=True)
        other = merge_split_table(other_label, other_values)
        df_list = [combusted, composted, recycled, landfilled, other]

        # Connecting to database
        conn = sqlite3.connect('wasted_data.sqlite')
        cur = conn.cursor()

        # Creating disposal table
        cur.execute("""CREATE TABLE disposal (
                            id INTEGER PRIMARY KEY,
                            disposal_type TEXT);
                    """)
        populate_disposal_table(cur)

        # Creating material table
        cur.execute("""CREATE TABLE material (
                            id INTEGER PRIMARY KEY,
                            product INTEGER,
                            material_type TEXT,
                            material_subtype TEXT);          
                    """)
        populate_material_table(cur)
        
        years = list(combusted.columns)
        years = list(map(int, years))

        # Creating disposal table
        cur.execute("""CREATE TABLE waste (
                            id INTEGER PRIMARY KEY,
                            material_id INTEGER,
                            disposal_id INTEGER,
                            year INTEGER,
                            waste_in_tons INTEGER);          
                    """)
        populate_waste_table(df_list, years, cur)

        # Saving csvs of data
        disposal = pd.read_sql("""SELECT * FROM disposal""", conn)
        disposal.to_csv('disposal_table.csv', index=False)
        material = pd.read_sql("""SELECT * FROM material""", conn)
        material.to_csv('material_table.csv', index=False)
        waste = pd.read_sql("""SELECT * FROM waste""", conn)
        waste.to_csv('waste_table.csv', index=False)

        # Committing changes and closing database
        conn.commit()
        conn.close()

        # Testing data
        assert(disposal.shape == (10,2))
        assert(material.shape == (13,4))
        assert(waste.shape == (396,5))

