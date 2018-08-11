<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
            <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation/ChemicalList/Chemical/NameOfSubstance">
            <Chemical>
                <PMID><xsl:value-of select="../../../PMID"/></PMID>
                <xsl:if test="../RegistryNumber">
                    <RegistryNumber><xsl:value-of select="../RegistryNumber"/></RegistryNumber>
                </xsl:if>
                <xsl:if test="@UI">
                    <UI><xsl:value-of select="@UI"/></UI>
                </xsl:if>
                <NameOfSubstance><xsl:value-of select="."/></NameOfSubstance>
            </Chemical>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>