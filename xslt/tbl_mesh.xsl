<?xml version="1.0" encoding="UTF-8"?>

<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="xml" encoding="utf-8" indent="yes"/>
    
    <xsl:template match="PubmedArticleSet">
            <xsl:apply-templates select="PubmedArticle"/>
    </xsl:template>
    
    <xsl:template match="PubmedArticle">
        <xsl:for-each select="MedlineCitation/MeshHeadingList/MeshHeading">
            <Term>
                <PMID><xsl:value-of select="../../PMID"/></PMID>
                <xsl:if test="DescriptorName">
                    <DescriptorName><xsl:value-of select="DescriptorName"/></DescriptorName>
                </xsl:if>
                <xsl:if test="DescriptorName/@MajorTopicYN">
                    <MajorDNTopicYN><xsl:value-of select="DescriptorName/@MajorTopicYN"/></MajorDNTopicYN>
                </xsl:if>
                <xsl:if test="DescriptorName/@UI">
                    <MeshUI><xsl:value-of select="DescriptorName/@UI"/></MeshUI>
                </xsl:if>
                <xsl:if test="QualifierName">
                    <QualifierName><xsl:value-of select="QualifierName"/></QualifierName>
                    </xsl:if>
                <xsl:if test="QualifierName/@MajorTopicYN">
                    <QualifierNameMajorTopicYN><xsl:value-of select="QualifierName/@MajorTopicYN"/></QualifierNameMajorTopicYN>
                </xsl:if>
                <xsl:if test="QualifierName/@UI">
                    <QualifierNameUI><xsl:value-of select="QualifierName/@UI"/></QualifierNameUI>
                </xsl:if>
            </Term>
        </xsl:for-each>
        
    </xsl:template>
</xsl:stylesheet>