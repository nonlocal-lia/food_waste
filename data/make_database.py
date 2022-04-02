import tabula
import pandas as pd
import sqlite3
from os.path import exists

def clean_pdf_data(df, years):
    # Dealing with rows mistaken as headers
    if df.columns[1] != '1960':
        df = df.columns.to_frame().T.append(df, ignore_index=True)
    
    # Dealing with header and calculated total rows
    header_rows = []
    for i, val in enumerate(df.iloc[:,0]):
        if val == 'Metals':
            header_rows.append(i)
            # relabeling  metal data for clarity
            for n in range(1,4):
                df.iloc[i+n, 0] = df.iloc[i, 0] +" - "+df.iloc[i+n, 0]
        elif val == 'Other Wastes':
            header_rows.append(i)
        elif 'composted' in val:
            header_rows.append(i)
        elif 'Inorganic' in val and 'Inorganic Wastes' not in val:
            header_rows.append(i)
            #Fixing split label
            df.iloc[i+1, 0] = df.iloc[i, 0] +" "+ df.iloc[i+1, 0]
        elif 'Total' in val:
            header_rows.append(i)
    
    if len(header_rows):
        df.drop(header_rows, inplace=True)
    
    # renaming columns
    column_labels = ['Material']
    column_labels.extend(years)
    df.columns=column_labels

    # Filling missing values
    df.replace('Neg.', 0, inplace=True)
    df.fillna('null', inplace=True)
    
    # Resetting Index
    df.reset_index(inplace=True)
    df.drop(columns=['index'], inplace=True)
    return df

def merge_split_table(df1, df2):
    # column title is actually a value, creating row from column title
    if 'Food' in df1.columns[0]:
        df1 = df1.columns.to_frame().T.append(df1, ignore_index=True)
    if df2.columns[0] != '2018':
        df2 = df2.columns.to_frame().T.append(df2, ignore_index=True)
    
    # Fixing column name
    df1.columns = ['Material']
    df2.columns = ['2018']
    
    # Finding Error Rows
    error_rows = []
    for i, val in enumerate(df1['Material']):
        if 'Food' not in val:
            error_rows.append(i)
    error_rows
    
    # Fixing labels that interpreter treated as seperate lines
    for row in error_rows:
        df1.iloc[row-1,0] = df1.iloc[row-1,0] + ' ' + df1.iloc[row,0]

    # droping extra lines
    df1.drop(error_rows ,inplace=True)

    # resetting index
    df1.reset_index(inplace=True)
    df1.drop(columns=['index'], inplace=True)

    # Joining dfs
    output = pd.concat([df1, df2], axis=1)
    
    # dropping calculated row
    output.drop([output.shape[0]-1], inplace=True)
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

def populate_material_table(cursor, materials):
    # replacing asterixed label
    for i, material in enumerate(materials):
        if material == 'Other **':
            materials[i] = 'Other Product'
            
    # pulling out sub groups    
    products = materials[0:-3]
    metals = materials[2:5]
    
    # populating database
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

def populate_waste_table(df_dict, years, cursor):
    compost_id = pd.read_sql("""SELECT id FROM disposal WHERE disposal_type = 'composting'""", conn).iloc[0,0]
    for disposal_id, df in df_dict.items():
        oth = False
        if disposal_id == 5:
            oth = True

        if oth:
            # Populating database
            material_id = pd.read_sql("""SELECT id FROM material WHERE material_type = 'Food'""", conn).iloc[0,0]
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
        else:
            for row in df.index:
                if disposal_id == compost_id:
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
    return None
        

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

        # Extracting years from columns
        years = list(combusted.columns)
        years.remove('Unnamed: 0')
        years

        # Cleaning anomolies from tabula
        recycled = clean_pdf_data(recycled, years)
        composted = clean_pdf_data(composted, years)
        combusted = clean_pdf_data(combusted, years)
        landfilled = clean_pdf_data(landfilled, years)
        other = merge_split_table(other_label, other_values)
        df_dict = {1: combusted, 2: composted, 3: recycled, 4: landfilled, 5: other}

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
        materials = list(combusted.iloc[:,0])
        populate_material_table(cur, materials)

        # Creating disposal table
        cur.execute("""CREATE TABLE waste (
                            id INTEGER PRIMARY KEY,
                            material_id INTEGER,
                            disposal_id INTEGER,
                            year INTEGER,
                            waste_in_tons INTEGER);          
                    """)
        populate_waste_table(df_dict, years, cur)

         # Testing data
        disposal = pd.read_sql("""SELECT * FROM disposal""", conn)
        material = pd.read_sql("""SELECT * FROM material""", conn)
        waste = pd.read_sql("""SELECT * FROM waste""", conn)
        assert(disposal.shape == (10,2))
        assert(material.shape == (13,4))
        assert(waste.shape == (396,5))
        
        # Saving csvs of data
        disposal.to_csv('disposal_table.csv', index=False)
        material.to_csv('material_table.csv', index=False)
        waste.to_csv('waste_table.csv', index=False)

        # Committing changes and closing database
        conn.commit()
        conn.close() 