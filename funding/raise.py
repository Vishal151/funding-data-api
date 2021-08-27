import os
import pandas as pd
import numpy as np
from funding.data import VCFunded

class Raise:
    '''
    DataFrames containing feature engineering,
    and various features as columns
    '''

    def __init__(self):
        # Assign an attribute ".data" to all new instances of Raise
        self.data = VCFunded().get_data()

    def clean_funding_data(self):
        """
        Cleaning the raw data file - remove duplicates, rename columns, deal with NAs and mis-labelled data
        """
        df = self.data['data'].copy()
        df = df.drop_duplicates()

        df.rename(columns = {'DATE RAISE ANNOUNCED':'raise_date','COMPANY':'company_name',
            'AMOUNT':'raise_amount_mill_dollars', 'HQ Location':'company_hq_location', 'TOP INVESTORS (in this round)':'raise_top_investors', 
            'LINK':'raise_link', 'Website':'company_website', 'Round ':'company_funding_round', 'Category': 'company_category', 'NOTES':'company_description',
            'Expansion Plans':'company_expansion_plans', 'Founder First Name':'founder_first_name', 'Founder Last Name':'founder_last_name', 
            'Founder LinkedIn':'founder_linkedin', 'Founder Twitter':'founder_twitter', 'Founder AngelList':'founder_angelist',
            'Unnamed: 16':'founder_email'},inplace=True)
        
        df = df[df['company_funding_round'].notna()] # select rows where funding_round is not empty
        df = df.reset_index(drop=True) # reset index and remove old one

        # Replace comma values so csv separation works
        df['company_hq_location'] = df['company_hq_location'].str.replace(',',';')
        df['raise_top_investors'] = df['raise_top_investors'].str.replace(',',';')
        df['company_description'] = df['company_description'].str.replace(',',';')
        df['company_expansion_plans'] = df['company_expansion_plans'].str.replace(',',';')
        df['founder_first_name'] = df['founder_first_name'].str.replace(',',';')
        df['raise_link'] = df['raise_link'].str.replace(',',' ')
        df['company_name'] = df['company_name'].str.replace(',',' ')

        # For the numeric column remove characters, change to integer and scale to millions
        df['raise_amount_mill_dollars'] = df['raise_amount_mill_dollars'].map(lambda x: x.replace('$',''))
        df['raise_amount_mill_dollars'] = df['raise_amount_mill_dollars'].map(lambda x: x.replace(',',''))
        df['raise_amount_mill_dollars'] = pd.to_numeric(df['raise_amount_mill_dollars']).astype(np.int64)
        df['raise_amount_mill_dollars'] = df['raise_amount_mill_dollars'].map(lambda x: x/1_000_000)

        # Convert raise date type, back fill any missing dates and format for Postgres
        df['raise_date'] = pd.to_datetime(df['raise_date'], errors='coerce', format="%m/%d/%Y")
        df['raise_date'] = df['raise_date'].bfill(axis=0)
        df['raise_date'] = df['raise_date'].dt.strftime('%Y-%M-%d')

        # Replace mis-typed categories
        df['company_category'].replace({
                'Agtech':'Agritech', 'Fermtech':'Femtech', 'COnsumer':'Consumer',
                'Foodtecvh':'Foodtech', 'INsurance':'Insurance','biotech/Health':'Biotech/Health',
                'insurtech':'Insurtech','ADtech/Martech':'Adtech/Martech','FIntech':'Fintech', 
                'enterprise':'Enterprise','Analyitcs':'Analytics', 'Consumert':'Consumer', 
                'Enteprrise':'Enterprise', 'Cosnumer':'Consumer', 'Energyteech':'Energytech',
                'industrial':'Industrial', 'INdustrial':'Industrial','Aerospacee':'Aerospace',
                'consumer':'Consumer', 'Ai/ML':'AI/ML', 'DevOPs':'DevOps', 
                'cannabis':'Cannabis', 'Data/Analtics':'Data/Analytics','Aadtech/Martech':'Adtech/Martech', 
                'ENterprise':'Enterprise', 'CLimatetech':'Climatetech', 'BIotech/Health':'Biotech/Health', 
                'Transportationn':'Trasportation', 'INsurtech':'Insuretech', 'Aeroospace':'Aerospace', 
                'Coonsumer':'Consumer', 'Logistcs':'Logistics','Fitnech':'Fintech', 
                'DEvOps':'Devops','Transportaiton':'Transportation', 'Transportatioon':'Transportation',
                'AI//ML':'AI/ML', 'Adtech/Marthech':'Adtech/Martech','Cybersercurity':'Cybersecurity', 
                'Insurtech ':'Insurtech','transportation':'Transportation', 'Consumer ':'Consumer',
                'Cyber-Security':'Cybersecurity', 'Ed-tech':'Edtech','Ed-Tech':'Edtech', 'Agech':'Agritech',
                'Cybsersecurity':'Cybersecurity','Roboticis':'Robotics','Finteech':'Fintech',
                'Eneterprise':'Enterprise', 'Cybesecurity':'Cybersecurity','Ai/mL':'AI/ML', 
                'Adtech/Marthec':'Adtech/Martech','BIOtech/Health':'Biotech/Health', 'data/Analytics':'Data/Analytics',
                'fintech':'Fintech', 'BLockchain':'Blockchain','Ccannabis':'Cannabis', 'robotics':'Robotics', 
                'Spacetechc':'Spacetech', 'Data/Analttics':'Data/Analytics','Consumer Internnet':'Consumer Internet', 
                'adtech/Martech':'Adtech/Martech', 'Biotech/Healthy':'Biotech/Health', 'COnsumer Internet':'Consumer Internet', 
                'Space':'Spacetech', 'DevOpds':'DevOps', 'Enterpris':'Enterprise','Consuner Internet':'Consumer Internet', 
                'Lifestyler':'Lifestyle','SpaceTech':'Spacetech','Govtech':'GovTech','Insuretech':'Insurtech',
                'Regtech':'RegTech', 'Enterprise Solution':'Enterprise Solutions','Martech':'Adtech/Martech', 
                'Transporatation':'Transportation','Biotech/Helath':'Biotech/Health', 'Devops':'DevOps',
                'Cybersecurity ':'Cybersecurity','Consumer INternet':'Consumer Internet', 'CYbersecurity':'Cybersecurity',
                'Ai/Machine Learning':'AI/ML', 'consumer Internet':'Consumer Internet', 'Enterprising':'Enterprise Solutions',
                'DAta/Analytics':'Data/Analytics', 'Enterprise solutions':'Enterprise Solutions','AI/Machine Learnign':'AI/ML',
                'bIOtech/Health':'Biotech/Health','bIotech/Health':'Biotech/Health', 'DEv':'Dev',
                'Quantum Computing ':'Quantum Computing','AI/Machine Leaning':'AI/ML', 'gaming':'Gaming',
                'Data/StorageAnalytics':'Data/Analytics', 'Consumer Interet':'Consumer Internet', 'Devops':'DevOps',
                'Cybersecuity':'Cybersecurity', 'AI/Machine Learing':'AI/ML', 'Data/Anlalytics':'Data/Analytics',
                'CLeantech':'Cleantech', 'Adtech':'Adtech/Martech', 'Footech':'Foodtech', 'COnsumer INternet':'Consumer Internet',
                'Bioetech/Health':'Biotech/Health', 'Data':'Data/Analytics', 'edtech':'Edtech', 'AR/Vr':'AR/VR', 
                'Enteprise':'Enterprise', 'Industry.40':'Industry4.0', 'Biotech/Heath':'Biotech/Health', 
                'Cyberecurity':'Cybersecurity', 'Food Tech':'Foodtech', 'Blochchain':'Blockchain', 'Gamng':'Gaming',
                'Customer Internet':'Consumer Internet', 'Entperprise':'Enterprise', 'PRoptech':'Proptech',
                'Real Esate':'Real estate', 'Trasportation':'Transportation', 'Edtec':'Edtech',
                'Ar/VR':'AR/VR', 'LIfestyle':'Lifestyle', 'lifestyle':'Lifestyle', 'Isurtech':'Insurtech',
                'Cleawntech':'Cleantech', 'Consumer Internt':'Consumer Internet', 'industral':'Industrial',
                'Industrials':'Industrial', 'Data/Analytis':'Data/Analytics', 'proptech':'Proptech',
                'Cybersecurit':'Cybersecurity', 'SPacetech':'Spacetech', 'REal Estate':'Real estate',
                'Insurteh':'Insurtech', 'INdustry4.0':'Industry4.0', 'RObotics':'Robotics',
                'Customer Products':'Consumer Products', 'Cybersecurirty':'Cybersecurity','cOnsumer Internet':'Consumer Internet',
                'Consumer Intenet':'Consumer Internet', 'Enterpise':'Enterprise', 'Biotech/Heatlh':'Biotech/Health',
                'Industral':'Industrial', 'dev':'Dev', 'Clean Tech':'CleanTech', 'Consumer Goods': 'Consumer Products',
                'Socialtecch':'Socialtech', 'blockchain':'Blockchain', 'AIML':'AI/ML', 'COnsumer Products':'Consumer Products',
                'FOodtech':'Foodtech', 'Gamers':'Gaming', 'Consumner Internet':'Consumer Internet',
                'Insrutech':'Insurtech', 'Insuretech':'Insurtech', 'Real estate':'Real Estate',
                'Real Estate Tech':'Proptech', 'Proptech':'PropTech','FinTech':'Fintech', 'Transporation':'Transportation',
                'TravelTech':'Traveltech','Telecom':'Telecoms', 'CleanTech':'Cleantech',
                'ClimateTech':'Climatetech','GreenTech':'Greentech', 'LegalTech':'Legaltech', 
                'SportTech':'SportsTech', 'Sportstech':'SportsTech','Sporttech':'SportsTech', 'DeepTech':'Deeptech',
                'Socialtech':'SocialTech','SalesTech':'Salestech', 'Biotech/Health ':'Biotech/Health',
                'Analytics':'Data/Analytics', 'Robots':'Robotics', 'Consumer products':'Consumer Products', 
                'Insuretech ':'Insurtech', 'Devops':'DevOps', 'Proptech':'PropTech', 'AI/Machine Learning': 'AI/ML'
        }, inplace=True)

        return df

    def save_clean_data(self):
        """
        Saves the clean data and cut down version for Postgres
        """
        df = self.clean_funding_data()

        save_url_clean = "data/clean/funding_data_clean.csv"
        save_url_cleancut = "data/clean/funding_data_cleancut.csv"

        df.to_csv(save_url_clean)
        df= df.head(9500) # Keep max 10K entries for free heroku tier
        df.to_csv(save_url_cleancut)


    def get_clean_data(self):
        """
        Returns a clean DataFrame (without NaN), with the following columns:
        ['raise_date', 'company_name', 'raise_amount_mill_dollars', 'company_hq_location',
         'raise_top_investors', 'raise_link', 'company_website', 'company_funding_round',
         'company_category', 'company_description', 'company_expansion_plans', 'founder_first_name',
         'founder_last_name', 'founder_linkedin', 'founder_twitter', 'founder_angelist', 'founder_email']
        """
        training_data = self.clean_funding_data()
        self.save_clean_data()
        return training_data

if __name__ == "__main__":
    Raise().get_clean_data()