<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
        <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation">
            <PubmedArticle>
                <PMID><xsl:value-of select="PMID"/></PMID>
                <xsl:if test="../PubmedData/ArticleIdList/ArticleId[starts-with(@IdType, 'pmc')]">
                    <PMC><xsl:value-of select="../PubmedData/ArticleIdList/ArticleId[starts-with(@IdType, 'pmc')]"/></PMC>
                </xsl:if>
                <xsl:if test="Article/Journal/JournalIssue/PubDate">
                    <PubDate>
                        <xsl:if test="Article/Journal/JournalIssue/PubDate/Year"><xsl:value-of select="Article/Journal/JournalIssue/PubDate/Year"/></xsl:if>
                        <xsl:if test="Article/Journal/JournalIssue/PubDate/Month">-<xsl:value-of select="Article/Journal/JournalIssue/PubDate/Month"/></xsl:if>
                        <xsl:if test="Article/Journal/JournalIssue/PubDate/Day">-<xsl:value-of select="Article/Journal/JournalIssue/PubDate/Day"/></xsl:if>
                        <xsl:if test="Article/Journal/JournalIssue/PubDate/MedlineDate"><xsl:value-of select="Article/Journal/JournalIssue/PubDate/MedlineDate"/></xsl:if>   
                    </PubDate>
                </xsl:if>
                <ArticleTitle><xsl:value-of select="Article/ArticleTitle"/></ArticleTitle>
                <JournalTitle><xsl:value-of select="Article/Journal/Title"/></JournalTitle>
                <xsl:if test="Article/Journal/JournalIssue/Volume">
                    <Volume><xsl:value-of select="Article/Journal/JournalIssue/Volume"/></Volume>
                </xsl:if>
                <Issue><xsl:value-of select="Article/Journal/JournalIssue/Issue"/></Issue>
                <xsl:if test="Article/Pagination">
                    <Pagination>
                        <xsl:if test="Article/Pagination/StartPage">
                            <xsl:value-of select="Article/Pagination/StartPage"/>-<xsl:value-of select="Article/Pagination/EndPage"/>
                        </xsl:if>
                        <xsl:if test="Article/Pagination/MedlinePgn">
                            <xsl:value-of select="Article/Pagination/MedlinePgn"/>
                        </xsl:if>
                    </Pagination>
                </xsl:if>
                <Abstract><xsl:for-each select="Article/Abstract/AbstractText">
                    <xsl:if test="@NlmCategory">
                        <!-- <xsl:text> </xsl:text> --><xsl:value-of select="@NlmCategory"/><xsl:text>: </xsl:text>
                    </xsl:if>
                    <xsl:value-of select="."/> </xsl:for-each></Abstract>
                <DateCreated><xsl:value-of select="DateRevised/Year"/>-<xsl:value-of select="DateRevised/Month"/></DateCreated>
                <DateCreatedYear><xsl:value-of select="DateRevised/Year"/></DateCreatedYear>
                <PubModel><xsl:value-of select="Article/@PubModel"/></PubModel>
                <xsl:if test="Article/ArticleDate/@DateType">
                    <ArticleDateType><xsl:value-of select="Article/ArticleDate/@DateType"/></ArticleDateType>
                </xsl:if>
                <xsl:if test="Article/ArticleDate/Year">
                    <ArticleDateYear><xsl:value-of select="Article/ArticleDate/Year"/></ArticleDateYear>
                </xsl:if>
                <CitationOwner><xsl:value-of select="@Owner"/></CitationOwner>
                <CitationStatus><xsl:value-of select="@Status"/></CitationStatus>
                <CitedMedium><xsl:value-of select="Article/Journal/JournalIssue/@CitedMedium"/></CitedMedium>
                <Language><xsl:value-of select="Article/Language"/></Language>
                <JournalNlmUniqueID><xsl:value-of select="MedlineJournalInfo/NlmUniqueID"/></JournalNlmUniqueID>
                <JournalCountry><xsl:value-of select="MedlineJournalInfo/Country"/></JournalCountry>
                <xsl:if test="MeshHeadingList/MeshHeading">
                    <MeSH><xsl:for-each select="MeshHeadingList/MeshHeading"><xsl:value-of select="DescriptorName"/>; </xsl:for-each></MeSH>
                </xsl:if>
            </PubmedArticle>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>