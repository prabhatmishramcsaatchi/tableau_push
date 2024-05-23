import json
import pandas as pd
import numpy as np
import boto3
import io   
from sqlalchemy import create_engine, text
import psycopg2
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import sqlalchemy
from deep_translator import GoogleTranslator
import ftfy
    
s3 = boto3.client('s3') 
                                                    
#translator = GoogleTranslator(source='auto', target='en')
# Initialize the translator
#translator = Translator()
DATABASE_URL = "postgresql://tableausprinklr:HiXy074Hi@prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com:5432/sprinklrproddb"
source_bucket = 'wikitablescrapexample'
#source_prefix = 'amazon_sprinklr_pull/tableau_layer/2023.0/masterbenchmarks_2023-08-22_2023-09-26.csv'
column_alignment_file='amazon_sprinklr_pull/mappingandbenchmark/ColumnAlignment.csv'
destination_bucket = 'wikitablescrapexample'
   # Database configurations
db_endpoint = 'prod-fluency-sprinklr-db-eu2.cpmfagf94hdi.eu-west-2.rds.amazonaws.com'
db_name = 'sprinklrproddb'
db_user = 'tableausprinklr'
db_password = 'HiXy074Hi'
data_table = 'sprinklr_test'
import_table = 'import_table'  
connection_string = f"postgresql://{db_user}:{db_password}@{db_endpoint}/{db_name}"
                                 
column_types = {
    'Date': sqlalchemy.types.DateTime(),
    'Dateoflastedit': sqlalchemy.types.DateTime()
}
    
twitter_mapping = {
    'Twitter Consumer': 'X(Twitter) Consumer',
    'Twitter News': 'X(Twitter) News',
    'Twitter': 'X(Twitter) News'
}
     
values_to_remove = [
    'AMZ XCM Brand (11679)', 'AMZ XCM Brand (Amazon)',
    'AMZ XCM Prime (11679)', 'AMZ XCM Prime (Amazon)',
    'AMZ XCM Prime: Prime Day 2023 (11679)', 'XCM Brand Social (Amazon Marketing)',
    'Positive Engagements Team (4329)','AMZ XCM Brand  (11679)'
    ]     
     
platform_mapping = {
    'X(Twitter) Consumer': 'X',
    'X(Twitter) News': 'X',
    'X(Twitter)': 'X',
    'Instagram Story': 'Instagram',
    'Instagram Reels': 'Instagram',
    'Instagram': 'Instagram',
    'Facebook': 'Facebook',
    'YouTube': 'YouTube',
    'WeChat': 'WeChat',
    'LinkedIn':'LinkedIn',
    'TikTok':'TikTok'
}
    
 
Parent_Platform_Category = {
    'X': 'X',
    'Facebook': 'Meta',
    'Instagram': 'Meta',
    'YouTube': 'YouTube',
    'WeChat': 'WeChat',
    'LinkedIn':'LinkedIn',
    'TikTok':'TikTok'
}
 
selected_accounts = ['Amazon IG - IN', 'Amazon FB - IN']
country_mapping_key = 'amazon_sprinklr_pull/mappingandbenchmark/account_metadataa.xlsx'
# Create an engine instance
engine = create_engine(connection_string)
def read_csv_from_s3(bucket, key):
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    csv_data = s3_object['Body'].read().decode('utf-8')
    data = pd.read_csv(io.StringIO(csv_data))  # Use io.StringIO here
    return data
    

     
      
    
def calculate_cpm(row):
    if (('CPM' in [row['Key Metric'], row['Key Metric 2'], row['Key Metric 3']]) and 
        row['Delivery'] == 'Paid Only' and 
        row['Paid Spend\n($)'] > 0 and 
        row['Total Impressions'] > 0):
        return (row['Paid Spend\n($)'] / row['Total Impressions']) * 1000
    else:
        return row['CPM']  # Return existing CPM value if conditions are not met

       
def benchmark_matcher(row):
    if row["Delivery"] != 'Paid Only':
        # Concatenate various fields, considering NA values
        return (row["Account"] if pd.notna(row["Account"]) else '') + \
               (row["Country"] if pd.notna(row["Country"]) else '') + \
               (row["Platform Granular"] if pd.notna(row["Platform Granular"]) else '') + \
               (row["Delivery"] if pd.notna(row["Delivery"]) else '') + \
               str(row["Year"]) + \
               str(row["Quarter"])
    else:
        # Concatenate 'Country' twice and other fields, considering NA values
        return (row["Country"] if pd.notna(row["Country"]) else '') * 2 + \
               (row["Platform Granular"] if pd.notna(row["Platform Granular"]) else '') + \
               (row["Delivery"] if pd.notna(row["Delivery"]) else '') + \
               str(row["Year"]) + \
               str(row["Quarter"]) + \
               (row["Paid Objective"] if pd.notna(row["Paid Objective"]) else '')


def preprocess_dataframe(df):
    #df['Translated Post'] = 'Translation Missing'
    df = df.rename(columns={'Video Views': 'Organic Video Views'})
    df['RowID'] = df['row_number']
    df['If yes who made it?'] = df['If yes who made it?'].fillna('N/A')
    df['Tier 1 Event?'] = df['Tier 1 Event?'].fillna('N/A')
        # Update 'Account' column based on 'is_paiddata'
    df.loc[(df['is_paiddata'] == 1) & (df['Account'].isnull()), 'Account'] = 'Paid'
    
    return df
 

def read_excel_from_s3(bucket, key, sheet_name):
    s3 = boto3.client('s3')  # Initialize an S3 client if not already done

    s3_object = s3.get_object(Bucket=bucket, Key=key)
    excel_data = s3_object['Body'].read()
    
    # Use io.BytesIO for Excel files
    excel_buffer = io.BytesIO(excel_data)
    
    # Read the Excel file from the buffer
    data = pd.read_excel(excel_buffer, sheet_name=sheet_name)
    
    return data

def merge_account_column(main_df, country_mapping_df):
    if main_df is not None and country_mapping_df is not None:
        # Check if necessary columns are present
        required_main_cols = {'Platform Granular', 'Account'}
        required_country_mapping_cols = {'Official Name', 'Platform Granular', 'Account'}
  
        if required_main_cols.issubset(main_df.columns) and required_country_mapping_cols.issubset(country_mapping_df.columns):

            # Create a concatenated and formatted column in both DataFrames
            main_df['formatted_account'] = (main_df['Account'] + main_df['Platform Granular']).str.replace(' ', '').str.lower()
 
            country_mapping_df['formatted_account'] = (country_mapping_df['Official Name'] + country_mapping_df['Platform Granular']).str.replace(' ', '').str.lower()

            # Create a mapping from the formatted account to the Interim Account
            account_mapping = dict(zip(country_mapping_df['formatted_account'], country_mapping_df['Account']))

            # Update the Account column in main_df where there is a match, keep original where there is no match
            main_df['Account'] = main_df['formatted_account'].map(account_mapping).fillna(main_df['Account'])
            

                

            # Drop the temporary formatted_account column
            main_df.drop(columns=['formatted_account'], inplace=True)


            return main_df
        else:
            return main_df
    else:
        return main_df
 
    

def calculate_metrics(df):
    def lookup_key (row):
        #ID if Paid or not
        if row['Delivery']=='Paid':
            return row['PERMALINK']+'PAID'
        else:
            return np.nan
    def key_metric (row):
        #Look up metric
        if pd.notnull(row["Key Metric"]):
            return row[row["Key Metric"]]
        else:
            return np.nan 
            
    def key_metric2 (row):
        #Look up metric
        if pd.notnull(row["Key Metric 2"]):
            return row[row["Key Metric 2"]]
        else:
            return np.nan
            
    def key_metric3 (row):
        #Look up metric
        if pd.notnull(row["Key Metric 3"]):
            return row[row["Key Metric 3"]]
        else:
            return np.nan
    def key_metric_bench (row):
        #Look up metric
        if pd.notnull(row["Key Metric"]):
            return row['Benchmark'+row["Key Metric"]]
        else:
            return np.nan
    def key_metric_bench2 (row):
        #Look up metric
        if pd.notnull(row["Key Metric 2"]):
            return row['Benchmark'+row["Key Metric 2"]]
        else:
            return np.nan
    def key_metric_bench3 (row):
        #Look up metric
        if pd.notnull(row["Key Metric 3"]):
            return row['Benchmark'+row["Key Metric 3"]]
        else:
            return np.nan
    #Calculated Fields
    def organic_beats_benchmark(row):
        #Check if BPS Blank
        if pd.notnull(row['Engagement Rate vs Benchmark(BPS)']):
            #Check organic or boosted
            if row['Organic & Boosted']==1:
                if row['Engagement Rate vs Benchmark(BPS)']>0:
                    return 1
                else:
                    return 0
            else:
                #Return Blank for non valid
                return np.nan
        #If BPS Blank return blank
        else:
            return np.nan
    def benchmark_paid(row):
        if row['Delivery']=='Paid' and pd.notnull(row['Paid Objective vs benchmark (+/-%)']):
            return 1
        else:
            return 0
    def paid_beats_bench(row):
        if row['Delivery']=='Paid':
            if row['Paid Objective vs benchmark (+/-%)']<0:
                return 1
            else:
                return 0
        else:
            return np.nan
    #Cleaning functions
    def paid_clean (row):
        if row['Delivery']=='Paid':
            return 'Paid Only'
        else:
            return row['Delivery']
    def paid_date_clean (row):
        if row['Delivery']=='Paid Only':
            return row['Pull Date']
        else:
            return row['Date']
    #Lookup Key
    df['Lookup Key']=df.apply(lookup_key, axis=1)
    #Key metric performance
    df['Paid Objective Result($)']=df.apply(key_metric, axis=1)
    df['Paid Objective Result 2($)']=df.apply(key_metric2, axis=1)
    df['Paid Objective Result 3($)']=df.apply(key_metric3, axis=1)
    df['Paid Objective vs benchmark (+/-%)']=df.apply(key_metric_bench, axis=1)
    df['Paid Objective vs benchmark 2(+/-%)']=df.apply(key_metric_bench2, axis=1)
    df['Paid Objective vs benchmark 3(+/-%)']=df.apply(key_metric_bench3, axis=1)
    #Column Calculations
    #Matcher
    df['Matcher']=df['Country']+df['Quarter']+df['Platform']+df['Delivery']
    #Organic & Boosted
    df['Organic & Boosted']=df['Delivery'].apply(lambda x: (x=='Organic') or (x=='Boosted')).astype(int)
    #Paid
    df['Paid']=df['Delivery'].apply(lambda x: (x=='Paid')).astype(int)
    #Benchmarked Paid
    df['Benchmark Paid'] = df.apply(benchmark_paid, axis=1)
    #Organic & Boosted Beats Benchmark
    df['Organic & Boosted Beats Benchmark'] = df.apply(organic_beats_benchmark, axis=1)
    #Historic Boosting
    df['Historic Boosting']=df['Delivery'].apply(lambda x: (x=='Historic Boosting (TW)')).astype(int)
    #Paid Beats Benchmark
    df['Paid Beats Benchmark'] = df.apply(paid_beats_bench, axis=1)
    #Holding Data
    df['Awareness CPM']=np.nan
    df['Non Awareness CPM']=np.nan
    df['Brand Awareness']=np.nan
    #df['CPM']=np.nan 
    df['Cost per Estimated Ad Recall Lift']=np.nan
    df['Estimated Ad Recall Lift']=np.nan
    df['CPM beats benchmark']=np.nan
    df['CPM v bench']=np.nan
    df['Cost per Estimated Ad Recall Lift beats benchmark']=np.nan
    df['Cost per est v bench']=np.nan
    df['Estimated Ad Recall Lift beats benchmark']=np.nan
    df['Ad recall v bench']=np.nan
    df['Video Views']=np.nan
    df['Cost per Thruplay']=np.nan
    df['Video Average Play Time']=np.nan
    df['Non-Awareness Campaign CPM']=np.nan
    df['Cost per Thruplay beats benchmark']=np.nan
    df['Cost per Thruplay v Bench']=np.nan
    df['Video Average Play Time beats benchmark']=np.nan
    df['Video Average Play v Bench']=np.nan
    df['Non-Awareness Campaign CPM beats benchmark']=np.nan
    df['Non Aware CPM V Bench']=np.nan
    df['Traffic']=np.nan
    df['CPC']=np.nan
    df['CTR']=np.nan
    df['Non-Awareness Campaign CPM2']=np.nan
    df['CPC beats benchmark']=np.nan
    df['CPC v Bench']=np.nan
    df['CTR beats benchmark']=np.nan
    df['CTR v bench']=np.nan
    df['Non-Awareness Campaign CPM beats benchmark3']=np.nan
    df['NonCPM v Bench']=np.nan
    df['Engagement']=np.nan
    df['CPE']=np.nan 
    df['Total Engagements']=np.nan
    df['Non-Awareness Campaign CPM3']=np.nan
    df['CPE beats benchmark']=np.nan
    df['CPEv bench']=np.nan 
    df['Total Engagements beats benchmark']=np.nan
    df['Engagements v bench']=np.nan
    df['NonAwareCPM v bench']=np.nan
    df['Non-Awareness Campaign CPM beats benchmark4']=np.nan
    df['EU5+TR']=np.nan
    df['Organic Corp & Ops Impressions']=np.nan
    df['Paid Corp & Ops Impressions2']=np.nan
    df['Boost Spend']=np.nan
    df['Dark Impressions']=np.nan
    df['Dark Spend']=np.nan
    df['Total Spend']=np.nan
    df['Consumer Boosted Impressions']=np.nan
    df['User Group']=np.nan
    
    #Final Fixes
    df['Delivery'] = df.apply(paid_clean, axis=1)
    df['Date'] = df.apply(paid_date_clean, axis=1)
    
    return df


  
def lookup_key(row):
    if row['Delivery'] in ['Paid Only', 'Historic Boosting (TW)']:
        return row['Permalink'] + row['Benchmark Type'] + str(row['Date']) + str(row['RowID']) + 'PAID'
    else:
        return row['Permalink'] + row['Benchmark Type'] + 'NONPAID'




        

def generate_upsert_query(data_table, import_table):
    columns_list = get_column_names(data_table)
    
    columns = ', '.join([f'"{col}"' for col in columns_list])
    update_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in columns_list])
    
    query = f"""
        INSERT INTO {data_table} ({columns})
        SELECT {columns} FROM {import_table}
        ON CONFLICT ("Lookup Key")
        DO UPDATE SET {update_clause};
    """
    
    return query
         
# def generate_upsert_query(data_table, import_table, df_columns):
#     columns_list = get_column_names(data_table)

#     # Ensure the order of columns in the DataFrame matches the database
#     df_columns = [col for col in columns_list if col in df_columns]
#     df = df[df_columns]

#     columns = ', '.join([f'"{col}"' for col in df_columns])
#     update_clause = ', '.join([f'"{col}" = EXCLUDED."{col}"' for col in df_columns])

#     query = f"""
#         INSERT INTO {data_table} ({columns})
#         SELECT {columns} FROM {import_table}
#         ON CONFLICT ("Lookup Key")
#         DO UPDATE SET {update_clause};
#     """

#     return query
           
                                                                                 
def add_unique_constraint():
    try:
        with engine.connect() as conn:
            conn.execute(text('ALTER TABLE sprinklr_test ADD UNIQUE ("Lookup Key");'))
            conn.commit()  # Commit the transaction
        print("Unique constraint added successfully!")
    except sqlalchemy.exc.ProgrammingError as pe:
        print(f"ProgrammingError: {pe}")
        conn.rollback()  # Rollback on error
    except Exception as e:
        print(f"Error adding unique constraint: {e}")
        conn.rollback()  # Rollback on error

 

def translate_text(text, should_translate=True):
    if not should_translate or text.lower() == 'instagram story':
        return text
    try:
        # Assuming GoogleTranslator is properly imported and initialized elsewhere
        translation = GoogleTranslator(source='auto', target='en').translate(str(text))
        return translation
    except Exception as e:
        print(f"Error translating text: {e}")
        return text  # Use the original text if translation fails
 
  
def get_column_names(data_table):
    query = text(f"SELECT * FROM {data_table} LIMIT 0")
    connection = engine.connect()
    try:
        result = connection.execute(query)
        columns_list = result.keys()
        return columns_list
    finally:
        connection.close()
   
       
def translate_dataframe(df):
    # Adjust the function to expect a DataFrame with 'Country' and 'Post' columns
    results = []
    with ThreadPoolExecutor() as executor:
        for _, row in df.iterrows():
            should_translate = row['Country'] not in ['AU', 'US', 'UK']
            result = executor.submit(translate_text, row['Post'], should_translate)
            results.append(result)
    
    translated_texts = [result.result() for result in results]
    df['Translated Post'] = translated_texts
    return df
                           
                                               
def lambda_handler(event, context):
    # Read data from S3
    # Read data from S3  
              
    key_event=event['Records'][0]['s3']['object']['key']
    #key_event='amazon_sprinklr_pull/tableau_layer/masterbenchmarks_2024-02-23_2024-04-08.csv'
 
    df = read_csv_from_s3(source_bucket, key_event)
    
    # Read the 'comment_sentiment.csv' file from S3 into the cmnt_sent dataframe
    cmnt_sent = read_csv_from_s3('wikitablescrapexample', 'amazon_sprinklr_pull/comment_sentiment/comment_sentiment.csv')
    
    grouped_url = cmnt_sent.groupby('threadURL')
    grouped_url_df = cmnt_sent.groupby('threadURL')['full_cat'].value_counts().unstack(fill_value=0).rename(columns={
        'positive': 'count_positive',
        'negative': 'count_negative',
        'neutral': 'count_neutral',
        'Spam/Irrelevant': 'count_Spam/Irrelevant',
        'Customer Service': 'count_Customer Service'
    }).reset_index()
 
         
        
     # Extract unique posts for translation
    df_translate = pd.DataFrame()
    df['Post'] = df['Post'].apply(lambda x: ftfy.fix_text(x) if pd.notnull(x) else x)

    df_translate = df[['Post', 'Country']].drop_duplicates().dropna(subset=['Post'])
   
    # Translate the posts
    translated_df = translate_dataframe(df_translate)
    translated_df = translated_df.drop(columns=['Country'])
    #translated_df=df_translate
    #translated_df['Translated Post']=" "
  
    
    # Merge the translated text back to the original dataframe
    df = df.merge(translated_df, on='Post', how='left')
    # Load country mapping data
    df_align = read_csv_from_s3(source_bucket, column_alignment_file)
    df = preprocess_dataframe(df)
    df = calculate_metrics(df)
    # Rename columns based on df_align
    for x in range(0, len(df_align)):
        df = df.rename(columns={df_align['Master Benchmarks'][x]: df_align['Final'][x]})
 
    # Select columns based on final_sql_format
     
    final_sql_format = df_align['Final'].tolist()
      
    
    df_final = df[final_sql_format]
     
         
    df_final['Date'] = pd.to_datetime(df_final['Date'], format='%Y-%m-%d')
    
    country_mapping = read_excel_from_s3(source_bucket,country_mapping_key, "Sheet1")

       
  
    # Apply the conditional filter
        # Replace infinite values with NaN in specific columns only Likes per Impression
       
 
  
    df_final['Likes per Impression \n(LPI)'].replace([np.inf, -np.inf], np.nan, inplace=True)
    df_final['Engagement Rate'].replace([np.inf, -np.inf], np.nan, inplace=True)
 
           
    # Add the 'Dateoflastedit' column with today's date
    df_final['Dateoflastedit'] = datetime.now().date()
          
      # Apply the lookup_key function to create a 'Lookup Key' column
    df_final['Lookup Key'] = df_final.apply(lookup_key, axis=1)
    
    df_final['Platform'] = df_final['Platform'].replace(twitter_mapping)
     
    df_final.rename(columns={'Platform': 'Platform Granular'}, inplace=True)
    df_final = merge_account_column(df_final, country_mapping)
    df_final['Consolidated Platform'] = df_final['Platform Granular'].replace(platform_mapping)
    df_final.rename(columns={'Consolidated Platform' : 'Platform'}, inplace=True)
    df_final['Holding Company'] = df_final['Platform'].replace(Parent_Platform_Category)
        # Step 3: Create 'Year' column by extracting the year from 'Date'
    df_final['Year'] = df_final['Date'].dt.year
    
    df_final.loc[df_final['Account'] == 'Amazon WeChat Video - CN', 'Platform Granular'] = 'WeChat Video'

 
    # Filter out rows where 'Account' is 'sell on amazon (in - english)' or ',sellersofamazon'
    df_final = df_final[~df_final['Account'].isin(['sell on amazon (in - english)', 'sellersofamazon'])]

    

  
    # Filter the DataFrame
    df_final = df_final[~df_final['Post Message'].isin(values_to_remove)]
 
    df_final['Date'] = pd.to_datetime(df_final['Date'])
 
    update_condition = (df_final['Country'] == 'CN')
    # Replace null values with an empty string
    df_final.loc[update_condition, ['Date', 'Benchmark Type', 'Permalink']] = df_final.loc[update_condition, ['Date', 'Benchmark Type', 'Permalink']].fillna('')
 
    # Update the RowID for rows where the Country is 'CN'
    df_final.loc[update_condition, 'RowID'] = (
    df_final.loc[update_condition, 'Date'].astype(str) +
    df_final.loc[update_condition, 'Country'] +
    df_final.loc[update_condition, 'Benchmark Type'] +
    df_final.loc[update_condition, 'Permalink']
    )   
    df_final = df_final[(df_final['Platform'] == 'WeChat') | ((df_final['Platform'] != 'WeChat') & (df_final['Date'] > '2023-09-04'))]
    df_final=df_final.reset_index()
    merged_df4 = pd.merge(df_final, grouped_url_df, left_on='Permalink', right_on='threadURL', how='left')
    df_final['Count-NegativeCommentSentiment'] = merged_df4['count_negative']
    df_final['Count-NeutralCommentSentiment'] = merged_df4['count_neutral']
    df_final['Count-PositiveCommentSentiment'] = merged_df4['count_positive']
    df_final['Count-CustomerServiceSentiment'] = merged_df4['count_Customer Service']
    df_final['Count-Spam/irrelevantComments'] = merged_df4['count_Spam/Irrelevant']
    
 
                
        # Condition for rows where 'Account' is 'Amazon LI - Global' and 'Platform Granular' is 'LinkedIn'
    condition = (df_final['Account'] == 'Amazon LI - Global') & (df_final['Platform Granular'] == 'LinkedIn')
    
    # Replace 'Global' in 'Account' with the 'Country' value for these rows
    df_final.loc[condition, 'Account'] = 'Amazon LI - ' + df_final.loc[condition, 'Country'].astype(str)
    
    df_final.loc[df_final['Delivery'] == 'Paid Only', 'Date'] = df_final.loc[df_final['Delivery'] == 'Paid Only', 'Date'] - pd.Timedelta(days=7)
    
    df_final = df_final[df_final['Region'] != 'MENA']
    
    df_final = df_final[df_final['Account'] != 'Amazon News X - EU']

    

       
    df_final = df_final[~((df_final['Country'] == 'US') & (~df_final['Post Message'].isin(['Marketing (Amazon Marketing)', 'GCCI Social Media: NA (Amazon Marketing)', 'GCCI Social Media: NA (4328)'])))]
    df_final = df_final[~(
    (df_final['Country'] == 'IN') &
    (df_final['Account'].isin(selected_accounts)) &
    (~df_final['Post Message'].isin(['GCCI Social Media: APAC (Amazon Marketing)', 'GCCI Social Media: APAC (4328)']))
    )]
    #df_final = df_final[~((df_final['Country'] == 'IN') & (~df_final['Post Message'].isin(['GCCI Social Media: APAC (Amazon Marketing)' , 'GCCI Social Media: APAC (4328)'])))]

 
    df_final['Benchmark Matcher'] = df_final.apply(benchmark_matcher, axis=1)
            
    # Apply the function to each row of the dataframe
   # df_final['CPM'] = df_final.apply(calculate_cpm, axis=1)
              
    # Drop duplicates based on the 'Lookup Key' column, keeping the last occurrence
    df_final.drop_duplicates(subset='Lookup Key', keep='last', inplace=True)
    #df_final = df_final[get_column_names(data_table)]
        # Fetch column names from the database
    db_columns = get_column_names(data_table)
    df_final = df_final[db_columns]
        
     
    #df_final = df_final[~df_final['Delivery'].isin(['Paid Only'])]
        
    # Add the unique constraint at the beginning of the lambda_handler
    add_unique_constraint()

    # Upload the DataFrame to the import_table in the database
    df_final.to_sql(import_table, engine, if_exists='replace', index=False, dtype=column_types)
    
      
     
  
    # Generate the upsert query
    upsert_query = generate_upsert_query(data_table, import_table)

    with engine.connect() as conn:
        # Start a new transaction
        trans = conn.begin()
        try:
            conn.execute(text(upsert_query))
            # Commit the transaction if all goes well
            trans.commit()
        except Exception as e:
            # Rollback the transaction in case of any exception
            trans.rollback()
            raise e
    engine.dispose()

    #df_final.to_sql(table_name, engine, index=False, if_exists='replace')
    # Save the resulting DataFrame as "finalsql.csv" in the source_bucket
    csv_buffer = io.StringIO()
    df_final.to_csv(csv_buffer, index=False)
    max_pull_date = df_final['Date'].max().strftime('%Y-%m-%d')
    s3.put_object(
         Bucket=source_bucket,
         Key=f'check/finalsql_{max_pull_date}.csv',  # Using f-string for cleaner formatting
         Body=csv_buffer.getvalue(),
    )
     
    
    return {
        'statusCode': 200,
        'body': json.dumps('Processing Completed!')
    }
#Organic,Boosted,Delivery
 
       
                     
