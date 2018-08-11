<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
            <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation/Article/GrantList/Grant">
            <Grant>
                <PMID><xsl:value-of select="../../../PMID"/></PMID>
                <GrantListCompleteYN><xsl:value-of select="../@CompleteYN"/></GrantListCompleteYN>
                <xsl:if test="GrantID">
                    <GrantID><xsl:value-of select="GrantID"/></GrantID>
                </xsl:if>
                <xsl:if test="Acronym">
                    <Acronym><xsl:value-of select="Acronym"/></Acronym>
                </xsl:if>
                <xsl:if test="Agency">
                    <Agency><xsl:value-of select="Agency"/></Agency>
                </xsl:if>
                <xsl:if test="Country">
                    <Country><xsl:value-of select="Country"/></Country>
                </xsl:if>
            </Grant>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>