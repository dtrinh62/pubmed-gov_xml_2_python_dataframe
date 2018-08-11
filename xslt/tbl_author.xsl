<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
            <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation/Article/AuthorList/Author">
            <Author>
                <PMID><xsl:value-of select="../../../PMID"/></PMID>
                <AuthorListCompleteYN><xsl:value-of select="../@CompleteYN"/></AuthorListCompleteYN>
                <AuthorValidYN><xsl:value-of select="@ValidYN"/></AuthorValidYN>
                <xsl:if test="LastName">
                    <LastName><xsl:value-of select="LastName"/></LastName>
                </xsl:if>
                <xsl:if test="ForeName">
                    <ForeName><xsl:value-of select="ForeName"/></ForeName>
                </xsl:if>
                <xsl:if test="Suffix">
                    <Suffix><xsl:value-of select="Suffix"/></Suffix>
                </xsl:if>
                <xsl:if test="Initials">
                    <Initials><xsl:value-of select="Initials"/></Initials>
                </xsl:if>
                <xsl:if test="CollectiveName">
                    <CollectiveName><xsl:value-of select="CollectiveName"/></CollectiveName>
                </xsl:if>
                <xsl:if test="EqualContrib">
                    <EqualContrib><xsl:value-of select="EqualContrib"/></EqualContrib>
                </xsl:if>
                <ConstructedPersonName><xsl:if test="LastName"><xsl:value-of select="LastName"/></xsl:if><xsl:if test="Suffix">, <xsl:value-of select="Suffix"/></xsl:if><xsl:if test="Initials">, <xsl:value-of select="Initials"/></xsl:if></ConstructedPersonName>
                <xsl:if test="AffiliationInfo/Affiliation">
                    <Affiliation><xsl:value-of select="AffiliationInfo/Affiliation"/></Affiliation>
                </xsl:if>
                <xsl:if test="AffiliationInfo/Identifier/@Source">
                    <IdentifierSource><xsl:value-of select="AffiliationInfo/Identifier/@Source"/></IdentifierSource>
                </xsl:if>
                <xsl:if test="AffiliationInfo/Identifier">
                    <Identifier><xsl:value-of select="AffiliationInfo/Identifier"/></Identifier>
                </xsl:if>
            </Author>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>