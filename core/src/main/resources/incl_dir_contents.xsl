<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="2.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:xi="http://www.w3.org/2001/XInclude" xmlns:fsxml="http://upennlib.edu/fsxml">
    <xsl:output method="xml" indent="yes"/>

    <xsl:template match="fsxml:file">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
            <xsl:if test="matches(@name, '\.((txt)|(log)|(xml))$')">
                <xsl:element name="xi:include">
                    <xsl:attribute name="href">
                        <xsl:text>file:</xsl:text>
                        <xsl:value-of select="@absolutePath"/>
                    </xsl:attribute>
                    <xsl:choose>
                        <xsl:when test="matches(@name, '\.xml$')">
                            <xsl:attribute name="parse">xml</xsl:attribute>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:attribute name="parse">text</xsl:attribute>
                        </xsl:otherwise>
                    </xsl:choose>
                    <xsl:element name="xi:fallback"/>
                </xsl:element>
            </xsl:if>
        </xsl:copy>
    </xsl:template>

    <xsl:template match="/*">
        <xsl:element name="dFlatRoot">
            <xsl:for-each select="@*">
                <xsl:choose>
                    <xsl:when test="compare(local-name(.),'name') = 0">
                        <xsl:attribute name="objectId" >
                            <xsl:value-of select="."/>
                        </xsl:attribute>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:copy/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:for-each>
            <xsl:apply-templates select="node()"/>
        </xsl:element>
    </xsl:template>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>

</xsl:stylesheet>
