#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 17 17:38:22 2018

@author: Dan Wendling

Last modified: 2018-08-11

Purpose: Ingest selected XML nodes from pubmed.gov data sets, File > XML exports,
into Python dataframe.


----------------
 NOTES/WARNINGS
----------------

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


-------------------------
 FIXME - RECOMMENDATIONS
-------------------------

* Need one function to re-use code, reduce chances for error.
* Need to have in the dataframes all possible features/columns wanted; 
currently what is brought into df is only the tags found in the current XML
file, too limiting.


----------------
SCRIPT CONTENTS
----------------

1. Start-up / What to put into place, where
2. Process/inspect pubmedarticle
3. Process/inspect author

Partly based on https://stackoverflow.com/questions/49439081/nested-xml-file-to-pandas-dataframe
"""


#%%
# ============================================
# 1. Start-up / What to put into place, where
# ============================================

import lxml.etree as et
import pandas as pd
import os

# Set working directory
# os.chdir('/Users/name/Projects/pubmed-gov_xml_2_python_dataframe')

# localDir = 'pubmed-gov_xml_2_python_dataframe/'


'''
For your file of database records, Go to pubmed.gov, run your strategy, 
and select Send to > File > Format: XML > Create File
'''

# Set XML and XSLT to your filenames
doc = et.parse("xml/Opioids-Caregivers.xml")
xsl_pubmedarticle = et.parse("xslt/tbl_pubmedarticle.xsl")
xsl_author = et.parse("xslt/tbl_author.xsl")


#%%
# ===================================
# 2. Process/inspect pubmedarticle
# ===================================

# Run the transform
transformer = et.XSLT(xsl_pubmedarticle)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

pubmedarticle = pd.DataFrame(data)

# Output to console
# print(result)

pubmedarticle.columns

# Reduce to what you need, reorder
pubmedarticle = pubmedarticle[['PMID', 'PMC', 'ArticleTitle', 'JournalTitle', 'Volume', 'Issue', 'Pagination', 'ArticleDateYear', 'DateCreatedYear', 'CitationStatus', 'Abstract', 'MeSH']]


# Article count by journal name
articleCountByJournalName = pubmedarticle['JournalTitle'].value_counts().reset_index()
articleCountByJournalName = articleCountByJournalName.rename(columns={'JournalTitle': 'Count', 'index': 'JournalTitle'})
articleCountByJournalName.head(n=20)

# Citation status (records are built incrementally)
# https://www.nlm.nih.gov/bsd/mms/medlineelements.html#stat
articleStatus = pubmedarticle['CitationStatus'].value_counts().reset_index()
articleStatus = articleStatus.rename(columns={'CitationStatus': 'Count', 'index': 'CitationStatus'})
articleStatus.head()

# Count of articles in PubMed Central
print("Available in PubMed Central (free full text)\n{}".format(pubmedarticle['PMC'].notnull().sum()))


#%%
# ===================================
# 3. Process/inspect author
# ===================================

transformer = et.XSLT(xsl_author)
result = transformer(doc)

data = []
for i in result.xpath('/*'):
    inner = {}
    for j in i.xpath('*'):
        inner[j.tag] = j.text

    data.append(inner)

author = pd.DataFrame(data)


# Output to console
# print(result)

author.columns
      
'''
Reduce to what you need, reorder. Will not bring in columns that are not in the
XML; potential list is PMID, AuthorListCompleteYN, AuthorValidYN, LastName,
ForeName, Suffix, Initials, CollectiveName, EqualContrib, ConstructedPersonName,
Affiliation, IdentifierSource, Identifier
'''
# FIXME - Useful to have Affiliation, but will cause error if it wasn't in the XML
author = author[['PMID', 'AuthorListCompleteYN', 'AuthorValidYN', 'LastName',
                 'ForeName', 'Initials', 'ConstructedPersonName']]
