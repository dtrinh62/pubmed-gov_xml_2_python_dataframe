<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
            <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation/Article/PublicationTypeList/PublicationType">
            <PublicationType>
                <PMID><xsl:value-of select="../../../PMID"/></PMID>
                <PublicationType><xsl:value-of select="."/></PublicationType>
                <xsl:if test="@UI">
                    <PublicationTypeUI><xsl:value-of select="@UI"/></PublicationTypeUI>
                </xsl:if>
            </PublicationType>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>