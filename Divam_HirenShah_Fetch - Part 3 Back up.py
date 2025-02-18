#!/usr/bin/env python
# coding: utf-8

# # Data Quality Findings and Checks

# In[1]:


import pandas as pd
import json
pd.set_option('display.max_columns', None)


# # Receipt Data Set

# In[2]:


receipt_data = pd.read_json('receipts.json', lines = True)
receipt_data.head()


# ### From the above, we can see we need to flatten the data further and pull out rewardsReceiptItemList as a separate dataframe. Once we get the data in readable dataframe format, we can perform quality checks further

# In[3]:


#Remove and flatten oid variables. 
def extract_oid(oid_dict):
    try:
        if isinstance(oid_dict, dict) and '$oid' in oid_dict:  # Check if it's a dict and has $oid
            return oid_dict['$oid']
        elif isinstance(oid_dict, str):  # If it's already a string, return it
            return oid_dict
        else:
            return None  
    except (TypeError, KeyError):  # Handles other unexpected types or missing keys
        return None



object_id_fields = ['_id', 'userId']

for field in object_id_fields:
    receipt_data[field] = receipt_data[field].apply(extract_oid)
    
# Flatten and convert date columns to dateTime. 
date_cols = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']
for col in date_cols:
    receipt_data[col] = pd.to_datetime(receipt_data[col].apply(lambda x: x['$date'] if isinstance(x, dict) else x), unit='ms')
receipt_data.head()


# ### We can now flatten the rewardsReceiptItemList as a separate date frame, since as per our model we would want it to be a separate table that holds the key for brand and receipt table. 

# ## Receipt Item Data Set

# In[5]:


df_exploded = receipt_data.explode("rewardsReceiptItemList")
receipt_item_data = pd.json_normalize(df_exploded["rewardsReceiptItemList"])
receipt_item_data.head()


# ## Receipt Joined With Receipt_Item Dataset

# In[6]:


receipt_joined_with_receipt_item = pd.concat([df_exploded.drop(columns=["rewardsReceiptItemList"]).reset_index(drop=True), receipt_item_data], axis=1)
receipt_joined_with_receipt_item.head()


# In[7]:


receipt_joined_with_receipt_item.shape


# ## Performing Data Quality Checks for Receipt Data Set

# In[8]:


receipt_data.info()


# ### From the above information about the receipt_data, we can infer the following: 
# 
# 1. Receipt Item List is missing for 440 reciepts. (Based on the assumption that every receipt, should have an item present in it)
# 2. Likewise, there are missing entries for other critical attributes like: total_spent, purchase_date. This will help stakeholders to understand what brands sell the most, avg spend value and other metrics that may frame future strategies. 
# 3. While it's okay to have missing or null values for bonus points, points earned, points awarded date (as not every item maybe eligigble for rewards or bonus rewards). In addition to that, finished_date so also be not null a every created_date for the receipt needs to have finished_date (irrespective of the processing result i.e Accepted or Rejected). 
# 4. We see all dates are in ms, however for readability to our analytics team, we can convert them into data_time format as done above. ids (receipt_id and user_id) can be string characters to keep the data consistent across all our datasets. 

# ## Summary Statistics for Receipt Data

# In[9]:


receipt_data.describe()


# ### Let's ensure receipt_id column does not have duplicates, as it's necessary for us to keep unique value

# In[10]:


duplicate_receipt_count = receipt_data['_id'].duplicated().value_counts()
print(duplicate_receipt_count)


# ## Performing Data Quality Checks for Receipt Item

# In[11]:


receipt_item_data.info()


# ### From the above we can see there are a lot of data discrepency. 
# 1. Every item that is being sold via a receipt, should have a bar_code and also brand_code. This will inturn tie up our data cleanly with the brands table. 
# 2. Every item list should have it's own item_line_id for uniqueness. This will prevent duplication. 
# 3. Consistent data formats for needsFetchReview -- boolean

# In[12]:


receipt_item_data.describe()


# # Users Data Set

# In[13]:


user_data = pd.read_json('users.json', lines = True)
user_data.head()


# In[14]:


object_id_fields = ['_id']

for field in object_id_fields:
    user_data[field] = user_data[field].apply(extract_oid)
    
# Flatten and convert date columns to dateTime. 
date_cols = ['createdDate', 'lastLogin']
for col in date_cols:
    user_data[col] = pd.to_datetime(user_data[col].apply(lambda x: x['$date'] if isinstance(x, dict) else x), unit='ms')


# In[15]:


user_data.head()


# ## Performing Data Quality Checks for Users

# In[16]:


user_data.info()


# ### Let's ensure _id or user_id is not duplicate as we would want to maintain a unique list of user_ids that have signed up in our database.

# In[17]:


duplicate_user_count = user_data['_id'].duplicated().value_counts()
print(duplicate_user_count)


# ### From the above we can infer and learn that
# 1. There are 283 duplicate user_ids created in our system. Meaning we have people with same information or some missing information being signed up multiple times in our system. We can prevent this from having right authentication and verification. To avoid inconsistencies and biased decision making. 
# 2. As far data formats go, we can make role, state, and signUpSource as strings. 

# # Brand Data Set

# In[18]:


brand_data = pd.read_json('brands.json', lines = True)
brand_data.head()


# In[19]:


### Extracting oid
for field in object_id_fields:
    brand_data[field] = brand_data[field].apply(extract_oid)
brand_data.head()


# ## Performing Data Quality Checks for Brands

# In[20]:


brand_data.info()


# ### Let's ensure brand_id or id is unqiue to the brand data set

# In[21]:


duplicate_brand_count = brand_data['_id'].duplicated().value_counts()
print(duplicate_brand_count)


# ### Checking Distinct Categorical Variables

# In[22]:


brand_data['category'].unique()


# In[23]:


brand_data['categoryCode'].unique()


# ### Learnings from the above:
# 1. topBrand can be a boolean instead of float
# 2. brandCode is null for some brands, however there are names present for all. This should be in sync and there can be data cleaning that can take place here. 
# 3. name, category, categoryCode, brandCode -- can be string.
# 4. cpg is another data set that can be extracted separately. (Refer to the ER & Data Model). However to keep it consice, I do not intend to expand and find quality issues in that. 

# ## Conclusion:

# ### There are multiple data cleaning steps required to be performed and executed after we have explored the data and understood the business goal for it. 
# 
# 1. If there are few rows with null values, we can enitrely drop them -- However this might not be useful for a criticial data set like ours which might lead to biased decision making. 
# 2. We can perform data imputation (replacing null or empty values with mean, median (incase of numerical data), mode (can be applied for numerical as well as categorical data). They can allow us to maintain the structure of our data model
# 3. We can also drop columns that are not very useful or may not answer the business questions directly. 
# 4. Remove outliers that might skew the data, this can be done via box-plot
# 5. We can also keep data types consistent, as there are inconsistencies detected.
# 6. We can also ensure input sanitization is carried out, to keep our user_input data clean and consistent
# 7. Last but not the least as a part of our data exploratory analysis, we can also plot histograms to bucket our categorical variables and see various trends for total spent for brands, items and receipt status to name a few
