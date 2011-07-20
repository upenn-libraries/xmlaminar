<?xml version="1.0" encoding="UTF-8"?>

<!DOCTYPE xsl:stylesheet [
 <!ENTITY cdata-start "&#xE501;">
 <!ENTITY cdata-end "&#xE502;">
]>

<xsl:stylesheet 
   xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
   xmlns:my="http://nowhere"
   xmlns:xlink="http://www.w3.org/1999/xlink" 
   xmlns:marc="http://www.loc.gov/MARC21/slim"
   xmlns:xs="http://www.w3.org/2001/XMLSchema"
   xmlns:integ="http://integrator"
   version="2.0" 
   exclude-result-prefixes="xsl marc xlink my">

 <xsl:output 
   encoding="UTF-8" 
   use-character-maps="cdata" 
   indent="yes" 
   method="xml" 
   version="1.0"
   omit-xml-declaration="no"/>

 <xsl:character-map name="cdata">
  <xsl:output-character character="&cdata-start;" string="&lt;![CDATA["/>
  <xsl:output-character character="&cdata-end;" string="]]>"/>
 </xsl:character-map>

 <xsl:template match="/">
  <add>
   <xsl:apply-templates select="integ:root/record"/>
  </add>
 </xsl:template>

 <xsl:template match="record">
  <!-- set navigation variables -->
  <xsl:variable name="bibmarc" select="./marc/marc:record"/>
  <xsl:variable name="holdings" select="./hldgs/hldg"/>
  <doc>
   <!-- DLA-specific format (determines cocoon, etc) -->
   <field name="format">record</field>

   <!-- Record ID -->
   <xsl:variable name="ID" select="$bibmarc/marc:controlfield[@tag='001']"/>
   <!--<xsl:variable name="ID" select="name($bibmarc/marc:controlfield[1]/@tag[1])"/>-->
   <field name="id">
    <xsl:value-of select="concat('FRANKLIN_', $ID)"/>
   </field>
   <field name="bibid_field">
    <xsl:value-of select="$ID"/>
   </field>

   <!-- author_creator_facet -->
   <!-- author_creator_search -->
   <!-- author_creator_sort -->
   <!-- author_creator_field -->
   <!-- Author searches should have fields both in current order, and re-inverted around the comma (e.g. "Joe Bui" and "Bui, Joe" -->
<xsl:variable name="fields_for_author" select="$bibmarc/marc:datafield[@tag=100 or @tag=110]"/>
<xsl:for-each select="$fields_for_author">
	<field name="author_creator_field"><xsl:value-of select="normalize-space(string-join(./marc:subfield[@code ne '6'], ' '))"/></field>
	<field name="author_creator_facet">
	<xsl:value-of select="normalize-space(string-join(./marc:subfield[@code ne '6'], ' '))"/>
	</field>
	<field name="author_creator_1_search">
	<xsl:value-of select="normalize-space(string-join(./marc:subfield[@code ne '6'], ' '))"/>
	</field>
	<field name="author_creator_1_search">
	<xsl:for-each select="./marc:subfield[@code='a']">
		<xsl:value-of select="substring-after(., ', ')"/><xsl:text> </xsl:text><xsl:value-of select="substring-before(., ',')"/>
	</xsl:for-each>
	<xsl:text> </xsl:text>
	<xsl:value-of select="normalize-space(string-join(./marc:subfield[@code ne 'a'][@code ne '6'], ' '))"/>
	</field>
</xsl:for-each>

<!-- TITLE -->
<field name="title_field"><xsl:value-of select="normalize-space($bibmarc/marc:datafield[@tag=245][1])"/></field>
<!-- marc record, for searching -->
<field name="marcrecord">
<xsl:text>&cdata-start;</xsl:text>
<xsl:copy-of select="$bibmarc"></xsl:copy-of>
<xsl:text>&cdata-end;</xsl:text>
</field>
  
</doc>
  
</xsl:template>

<xsl:template match="*"/>

</xsl:stylesheet>
