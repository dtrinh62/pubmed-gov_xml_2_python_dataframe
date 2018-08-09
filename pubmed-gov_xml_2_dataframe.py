#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 17:38:22 2018

@author: Dan Wendling

Last modified: 2018-08-09

Purpose: Ingest selected XML nodes from pubmed.gov data sets, File > XML exports,
into Python dataframe.

Notes/Warnings:

* Pubmed.gov data is hierarchical XML; this process removes hierarchy. The
XSL file here can generate the "pubmedarticle" table in a relational database.
Other XSL files could create one table each for authors, grants, MeSH, 
publication types, etc. - the fields that allow multiple entries. In that
case, set PMID as the primary key in this "table," and in the other tables
use PMID as the foreign key.
* This file: In case you want to analyze TI-AB-DE, aggregate by journal, 
access journal info from Catalog
* Some records will be left out! Nothing is captured for <PubmedBookArticle>, 
for example. The below captures journal articles, <PubmedArticle>.
* Structured abstracts use the NLM structure (@NlmCategory); abstract labels 
from the article are not included.
* Some elements are simplified. More items could be added - this is not 
everything.
* MeSH is thrown in here as one column, in case you want to use natural
language processing on TI-AB-MeSH, etc. The MeSH content ends with 
semi-colon space; code to prevent this is available (somewhere, not here).


----------------
SCRIPT CONTENTS
----------------
1. Start-up / What to put into place, where
2. Transform using XSLT file
3. Convert to dataframe
4. Re-order, reduce columns
5. Summarize

Steps 1-3 are based on 
https://stackoverflow.com/questions/49439081/nested-xml-file-to-pandas-dataframe
"""


#%%
# ============================================
# 1. Start-up / What to put into place, where
# ============================================

import lxml.etree as et
import pandas as pd

'''
For your file of database records, Go to pubmed.gov, run your strategy, 
and select Send to > File > Format: XML > Create File
'''

# Set XML and XSLT to your filenames
doc = et.parse("xml/Opioids-Caregivers.xml")
xsl = et.parse("xml/tbl_pubmedarticle.xsl")


#%%
# ===================================
# 2. Transform using XSLT file
# ===================================

# Run the transform
transformer = et.XSLT(xsl)
result = transformer(doc)

# Output to console
# print(result)


#%%
# ===================================
# 3. Convert to dataframe
# ===================================

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

df = pd.DataFrame(data)


#%%
# ===================================
# 4. Re-order, reduce columns
# ===================================

# View col names
df.columns

reduced_df = df[['PMID', 'PMC', 'ArticleTitle', 'JournalTitle', 'Volume', 'Issue', 'Pagination', 'ArticleDateYear', 'DateCreatedYear', 'CitationStatus', 'Abstract', 'MeSH']]


#%%
# ===================================
# 5. Summarize
# ===================================

# Article count by journal name
articleCountByJournalName = reduced_df['JournalTitle'].value_counts().reset_index()
articleCountByJournalName = articleCountByJournalName.rename(columns={'JournalTitle': 'Count', 'index': 'JournalTitle'})
articleCountByJournalName.head(n=20)

# Citation status (records are built incrementally)
# https://www.nlm.nih.gov/bsd/mms/medlineelements.html#stat
articleStatus = reduced_df['CitationStatus'].value_counts().reset_index()
articleStatus = articleStatus.rename(columns={'CitationStatus': 'Count', 'index': 'CitationStatus'})
articleStatus.head()

# Count of articles in PubMed Central
print("Available in PubMed Central (free full text)\n{}".format(reduced_df['PMC'].notnull().sum()))
