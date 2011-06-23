<?xml version="1.0" encoding="UTF-8"?>

<!-- Stylesheet for Franklin  -->

<!DOCTYPE xsl:stylesheet [
 <!ENTITY cdata-start "&#xE501;">
 <!ENTITY cdata-end "&#xE502;">
]>

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:my="http://nowhere"
 xmlns:xlink="http://www.w3.org/1999/xlink" xmlns:marc="http://www.loc.gov/MARC21/slim"
 xmlns:xs="http://www.w3.org/2001/XMLSchema"
 version="2.0">
 
 <xsl:output encoding="UTF-8" use-character-maps="cdata" indent="yes" method="xml" version="1.0"
  omit-xml-declaration="no"/>

 <xsl:character-map name="cdata">
  <xsl:output-character character="&cdata-start;" string="&lt;![CDATA["/>
 <xsl:output-character character="&cdata-end;" string="]]>"/>
 </xsl:character-map>


<xsl:template match="/">
<add>
<xsl:apply-templates select="marc:collection/marc:record"/>
</add>
</xsl:template>

<!--<xsl:template match="marc:record">
<doc>
<xsl:apply-templates select="marc"/>
<xsl:apply-templates select="holdings"/>
</doc>
</xsl:template>-->

<xsl:template match="holdings">
<xsl:apply-templates select="holding"/>
</xsl:template>

<xsl:template match="holding">
    <field name="call_number"><xsl:value-of select="display_call_no"/></field>
    <field name="location"><xsl:value-of select="perm_location"/></field>
    <!--<xsl:for-each select="items/item/itemStatuses/itemStatus">
        <field name="status"><xsl:value-of select="status"/></field>
    </xsl:for-each>-->
</xsl:template>
 
<xsl:template match="marc:record">
 <doc>
    <field name="format">record</field>
  
    <!-- Record ID -->
    <xsl:variable name="ID" select="marc:controlfield[@tag='001']"/>
    <field name="id">
        <xsl:value-of select="concat('FRANKLIN_', $ID)"/>
    </field>
    <field name="bibid">
        <xsl:value-of select="$ID"/>
    </field>
   
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <!-- SORT FIELDS -->
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   
   <!--  separate/remove leading article - used for correct indexing/searching of title -->
   <xsl:variable name="title_offset" select="marc:datafield[@tag=245]/@ind2"/>
   <field name="title_sort">
    <xsl:choose>
     <xsl:when test="$title_offset &gt; 0 and $title_offset &lt; 10">
      <!-- 245 $a minus the leading article -->
      <xsl:value-of select="substring(marc:datafield[@tag=245]/marc:subfield[@code='a'], $title_offset + 1)"/>
     </xsl:when>
     <xsl:otherwise>
      <!-- if there's no article (245 2nd ind) just take $a. if no $a, use $k. remove [ if at start of $k. 
       haven't accounted for if $k has 245 2nd ind. It shouldn't, but some do anyway --> 
      <xsl:value-of select="if (marc:datafield[@tag=245]/marc:subfield[@code='a'])
       then marc:datafield[@tag=245]/marc:subfield[@code='a']
       else (replace(marc:datafield[@tag=245]/marc:subfield[@code='k'],'^\[',''))"/>
     </xsl:otherwise>
    </xsl:choose>
    <xsl:text> </xsl:text>
    <xsl:value-of select="marc:datafield[@tag=245]/marc:subfield[@code='b']"/>
    <xsl:text> </xsl:text>
    <xsl:value-of select="marc:datafield[@tag=245]/marc:subfield[@code='n']"/>
    <xsl:text> </xsl:text>
    <xsl:value-of select="marc:datafield[@tag=245]/marc:subfield[@code='p']"/>
   </field>
   
   
   <!-- ++++++++++++++++++++++ -->
   <!-- LANGUAGE facet from 008  -->
   <xsl:variable name="languages" select="document('/home/michael/xslTesting/languages2.xml')"/>   
   <!--	<xsl:variable name="languages" select="document('file:/H:/My%20Documents/DLA/dla/languages2.xml')"/>  -->
   <xsl:variable name="lang008" select="substring(marc:controlfield[@tag=008],36,3)"></xsl:variable>
   <xsl:if test="not(matches($lang008, 'zxx|___|mul|und'))">
    <field name="language_facet">
     <xsl:value-of select="$languages/languages/lang[@code=$lang008]"></xsl:value-of>  
    </field>
   </xsl:if>
   
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <!-- BIB FORMAT facet -->
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <xsl:variable name="form" select="substring((leader), 7,2)"/>
   <xsl:choose>
    <!-- TODO: do we have to keep this? -->
     <!-- no matter what the format, if it's Manuscripts location give it format Manuscripts -->
<!--    <xsl:when test="matches($voyLocName, 'manuscripts', 'i')">
     <field name="format_facet"><xsl:text>Manuscripts</xsl:text></field>
    </xsl:when>
-->    
    <!-- split books format into books and ebooks -->	
    <!-- format_facet is just for facet list, displayFormat is for detailed view, and results list. For those we don't
     want to see 'Books - all' so we need the other view -->		
    <xsl:when test="matches($form, 'am')">
     <!--<field name="format_facet"><xsl:text>Books - all</xsl:text></field>-->
     <xsl:choose>
      <xsl:when test="matches(marc:datafield[@tag=245]/marc:subfield[@code='h'], 'electronic resource', 'i')
       and matches(substring(marc:controlfield[@tag=006][1],1,1), 'm')
       and matches(substring(marc:controlfield[@tag=007][1],1,2), 'cr')">
       <!--<field name="format_facet"><xsl:text>Books - electronic</xsl:text></field>-->
       <field name="displayFormat"><xsl:text>Books - electronic</xsl:text></field>
      </xsl:when>
      <xsl:otherwise>
       <!--<field name="format_facet"><xsl:text>Books - print</xsl:text></field>-->
       <field name="displayFormat"><xsl:text>Books - print</xsl:text></field> 
      </xsl:otherwise>
     </xsl:choose>
    </xsl:when>
    
    <xsl:when test="matches($form, 'cm')">
     <!--<field name="format_facet"><xsl:text>Music scores</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Music scores</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'dm')">
     <!--<field name="format_facet"><xsl:text>Music scores</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Music scores</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'em')">
     <!--<field name="format_facet"><xsl:text>Atlases and maps</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Atlases and maps</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'gm')">
     <!--<field name="format_facet"><xsl:text>Videorecordings</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Videorecordings</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'im')">
     <!--<field name="format_facet"><xsl:text>Nonmusical sound recordings</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Nonmusical sound recordings</xsl:text></field>
    </xsl:when> 
    <xsl:when test="matches($form, 'jm')">
     <!--<field name="format_facet"><xsl:text>Sound recordings</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Sound recordings</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'km')">
     <!--<field name="format_facet"><xsl:text>Graphic materials</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Graphic materials</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'mm')">
     <!--<field name="format_facet"><xsl:text>Datafiles</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Datafiles</xsl:text></field>
    </xsl:when>
    <!-- if tm, lump it in with paper books, so give it an All books and a Paper books only -->
    <xsl:when test="matches($form, 'tm')">
     <!--<field name="format_facet"><xsl:text>Books - all</xsl:text></field>
     <field name="format_facet"><xsl:text>Books - print</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Books - print</xsl:text></field>
    </xsl:when>
    <xsl:when test="matches($form, 'as')">
     <!--<field name="format_facet"><xsl:text>Journals and electronic journals</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Journals and electronic journals</xsl:text></field>
    </xsl:when>
    <!-- if ac, lump it in with paper books, so give it an All books and a Paper books only -->
    <xsl:when test="matches($form, 'ac')">
     <!--<field name="format_facet"><xsl:text>Books - all</xsl:text></field>
     <field name="format_facet"><xsl:text>Books - print</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Books - print</xsl:text></field>
    </xsl:when>
    <!-- starts with r (there is no r in the 2nd spot, char 7 in the leader)  -->
    <xsl:when test="matches($form, 'r')">
     <!--<field name="format_facet"><xsl:text>Three-dimensional objects</xsl:text></field>-->
     <field name="displayFormat"><xsl:text>Three-dimensional objects</xsl:text></field>
    </xsl:when> 
   </xsl:choose>
   
   <!-- ++++++++++++++++++++++++++++++++ -->
   <!-- AUTHOR  field 100 NR, $c R, others NR-->
   <xsl:if test="marc:datafield[@tag=100]">
    <field name="author">
     <xsl:for-each select="marc:datafield[@tag=100]/marc:subfield">
      <xsl:choose>
       <xsl:when test="@code='a'"><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='b'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='c'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='d'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='q'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='t'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
      </xsl:choose>
     </xsl:for-each>
    </field>
   </xsl:if>	
   
   <!-- 110 NR, some subf R, some NR -->
   <xsl:if test="marc:datafield[@tag=110]">
    <field name="author">
     <xsl:for-each select="marc:datafield[@tag=110]/marc:subfield">
      <xsl:choose>
       <xsl:when test="@code='a'"><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='b'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='c'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='d'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='f'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='h'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
       <xsl:when test="@code='t'"><xsl:text> </xsl:text><xsl:value-of select="my:trimOnlySpaces(.)"/></xsl:when>
      </xsl:choose>
     </xsl:for-each>
    </field>	
   </xsl:if>

   <!-- ++++++++++++++++++++++++++++++++ -->
   <!-- 245 TITLE -->
   <!-- ++++++++++++++++++++++++++++++++ -->
   <xsl:variable name="titlewithslash" select="if (marc:datafield[@tag=245][1]/marc:subfield[@code='a'])
    then marc:datafield[@tag=245][1]/marc:subfield[@code='a']
    else (replace(marc:datafield[@tag=245][1]/marc:subfield[@code='k'],'^\[',''))"></xsl:variable>
   
   <xsl:variable name="title_ak" select="my:trimTrailingComma(my:trimOnlySpaces(my:trimTrailingSlash($titlewithslash)))"/>
   
   <!-- get the punct at end, if any. substring starts counting at zero -->
   <xsl:variable name="len" select="string-length($title_ak)"></xsl:variable>
   <xsl:variable name="apunct" select="substring($title_ak, ($len cast as xs:double), 1)"></xsl:variable> 
   
   <xsl:variable name="len" select="string-length(my:trimOnlySpaces(marc:datafield[@tag=245][1]/marc:subfield[@code='h']))"></xsl:variable>
   <xsl:variable name="hpunct" select="substring(my:trimOnlySpaces(marc:datafield[@tag=245][1]/marc:subfield[@code='h']), ($len cast as xs:double), 1)"></xsl:variable>
   
   <xsl:variable name="punct" select="if ( matches($apunct, '=') or matches($hpunct, '=') ) then '=' else(
    if ( matches($apunct, ':') or matches($hpunct, ':') ) then ':' else() )"></xsl:variable> 
   
   
   <!-- 245 a punct b n p-->
   <field name="title_facet">
    <xsl:value-of select="my:trimOnlySpaces(concat(my:trimTrailingSemiColon(my:trimTrailingEqual($title_ak)), ' ', $punct, ' ', 
     my:trimTrailingSlash(marc:datafield[@tag=245][1]/marc:subfield[@code='b'][1]), ' ', 
     my:trimTrailingSlash(marc:datafield[@tag=245][1]/marc:subfield[@code='n'][1]), ' ', 
     my:trimTrailingSlash(marc:datafield[@tag=245][1]/marc:subfield[@code='p'][1])))"/>	
   </field>
   
   <!-- ++++++++++++++++++++++++++++++++ -->
   <!-- 880 TITLE and 880 AUTHOR fields -->
   <!-- turn the 880s into author and title fields if they exist -->	
   <!-- just $a for caption on list page, all subf for detailed view -->
   <xsl:for-each select="marc:datafield[@tag='880']">
    <!-- just $a -->
    <xsl:if test="starts-with(marc:subfield[@code='6'], '245')">
     <field name="title_880a">	
      <xsl:value-of select="my:trimTrailingSlash(my:trimTrailingSemiColon(my:trimWhitespace(marc:subfield[@code='a'])))"/>	
     </field>
    </xsl:if>
    <!-- just $a -->
    <xsl:if test="starts-with(marc:subfield[@code='6'], '246')">
     <field name="alttitle_880a">
      <xsl:value-of select="my:trimTrailingSemiColon(my:trimWhitespace(marc:subfield[@code='a']))"/>
     </field>
    </xsl:if>
   </xsl:for-each>
   
   <!-- ALTERNATE TITLE field  246 $a limit to 1st one -->
   <xsl:variable name="oneAltTitle" select="marc:datafield[@tag=246 and @ind1 = '3' and @ind2 = '1'][1]/marc:subfield[@code='a']"/>
   <xsl:if test="$oneAltTitle and not( $punct )">
    <field name="alttitle">
     <xsl:value-of select="$oneAltTitle"></xsl:value-of>
    </field>
   </xsl:if>
   
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <!-- SUBJECTS   for Facets -->
   <!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   
   <!--  full subject heading - the entire 6xx field, all subfields strung together -->	
   <xsl:for-each	select="marc:datafield[@tag=650  and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
    or @tag=651 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2') 
    or @tag=655 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')]">
    <xsl:if test="not(starts-with(my:trimOnlySpaces(.) , 'PRO ')) and not(starts-with(my:trimOnlySpaces(.) , 'CHR ')) ">
     <field name="subject_full_facet">
      <xsl:for-each select="marc:subfield[@code != '2' and @code != '6']">
       <xsl:choose>
        <xsl:when test="position()=last()">
         <xsl:value-of select="if (ends-with(., 'etc.')) then (.) else (my:trimTrailingPeriod(.))"> </xsl:value-of>
        </xsl:when>
        <xsl:when test="ends-with(my:trimOnlySpaces(.), 'etc.') or (not(ends-with(my:trimOnlySpaces(.), '.')) and 
         not(ends-with(my:trimOnlySpaces(.), ':')) and
         not(ends-with(my:trimOnlySpaces(.), ';')) and
         not(ends-with(my:trimOnlySpaces(.), ')')) and 
         not(ends-with(my:trimOnlySpaces(.), ',')) and
         not(ends-with(my:trimOnlySpaces(.), '?')) and
         not(ends-with(my:trimOnlySpaces(.), '-')))">
         <xsl:value-of select="concat(., ' - ')"/>
        </xsl:when>
        <xsl:when test="ends-with(my:trimOnlySpaces(.), '.') or 
         ends-with(my:trimOnlySpaces(.), ':') or
         ends-with(my:trimOnlySpaces(.), ';') or
         ends-with(my:trimOnlySpaces(.), ')') or 
         ends-with(my:trimOnlySpaces(.), ',') or
         ends-with(my:trimOnlySpaces(.), '?') or
         ends-with(my:trimOnlySpaces(.), '-') ">
         <xsl:value-of select="concat(., ' ')"/>
        </xsl:when>
        
       </xsl:choose>
      </xsl:for-each>
     </field>
    </xsl:if>
   </xsl:for-each>
   
   <!-- brief subject heading = subfield a in a string -->
   <xsl:for-each	select="marc:datafield[@tag=650  and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
    or @tag=651 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2') 
    or @tag=655 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')]/marc:subfield[@code = 'a']">
    <xsl:if test="not(starts-with(my:trimOnlySpaces(.) , 'PRO ')) and not(starts-with(my:trimOnlySpaces(.) , 'CHR '))">
     <field name="subject_brief_facet">
      <xsl:value-of select="if (ends-with(., 'etc.')) then (.) else (my:trimTrailingPeriod(.))"> </xsl:value-of>
     </field>
    </xsl:if>
   </xsl:for-each>
   
   <!-- subject keyword = every word of every 6xx $a is its own value -->
   <xsl:for-each	select="marc:datafield[@tag=650  and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
    or @tag=651 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2') 
    or @tag=655 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')]">
    <xsl:for-each select="marc:subfield[@code='a']">
     <xsl:if test="not(starts-with(my:trimOnlySpaces(.) , 'PRO ')) and not(starts-with(my:trimOnlySpaces(.) , 'CHR '))">
      <xsl:variable name="grp" select="tokenize(my:trimOnlySpaces(my:trimDoubleQuote(my:trimAngleBracket(my:trimParens(my:trimTrailingPeriod(.))))), ' ' )"></xsl:variable>
      
      <xsl:for-each-group select="$grp" group-by="my:trimTrailingComma(my:trimWhitespace(.))">
       <xsl:if test="not(matches(., '^\d')) and not(matches(., '^an$', 'i')) and not(matches(., '^and$', 'i')) and not(matches(., '^as$', 'i')) 
        and not(matches(., '^at$', 'i')) and not(matches(., '^by$', 'i')) and not(matches(., '^da$', 'i')) and not(matches(., '^de$', 'i'))  
        and not(matches(., '^del$', 'i')) and not(matches(., '^der$', 'i')) and not(matches(., '^des$', 'i')) and not(matches(., '^di$', 'i')) 
        and not(matches(., '^die$', 'i')) and not(matches(., '^do$', 'i')) and not(matches(., '^du$', 'i')) and not(matches(., '^et$', 'i')) 
        and not(matches(., '^etc$', 'i')) and not(matches(., '^for$', 'i'))
        and not(matches(., '^from$', 'i')) and not(matches(., '^II$', 'i')) and not(matches(., '^in$', 'i')) and not(matches(., '^la$', 'i')) 
        and not(matches(., '^les$', 'i')) and not(matches(., '^of$', 'i'))  and not(matches(., 'on$', 'i')) and not(matches(., '^to$', 'i'))
        and not(matches(., '^with$', 'i')) 
        and not(matches(., '&amp;')) and not(matches(., ':')) and not(matches(., '\+')) and not(matches(.,'^\w?$'))">
        <field name="subject_keyword_facet">
         <xsl:value-of select="my:initCap(current-grouping-key())"/>
        </field>
       </xsl:if>
      </xsl:for-each-group>
     </xsl:if>
    </xsl:for-each>
   </xsl:for-each>
   
   
   <!-- ++++++++++++++++++++++++++++++++ -->
   <!-- PUBLICATION DATE (YEAR)  from 008  -->
   <!-- in the marc rec we start counting from 0. 
    The chars are actually 11-14 and 7 - 10 in the marc record.
    12-15 and 8 - 11 in the xsl substring function -->
   <xsl:variable name="publicationYear" select="substring(marc:controlfield[@tag=008],8,4)"></xsl:variable>
   <xsl:variable name="currYear" select="format-date(current-date(), '[Y]') cast as xs:integer"></xsl:variable>
   
   <!-- want the publication date to display in search results page even if it has u in it -->
   <xsl:if test="$publicationYear ne '    '">
    <field name="publication_date">
     <xsl:value-of select="$publicationYear"/>
    </field>
   </xsl:if>
   
   <!-- for facet, only want all numeral date , grouped by decade until 2000, then single year -->
   <!-- decade from year variable -->
   <xsl:if test="matches($publicationYear, '\d\d\d\d')">
    <field name="publication_date_facet">
     <xsl:choose>
      <xsl:when test="$publicationYear lt '2000' ">
       <xsl:value-of select="concat(substring($publicationYear, 1,3), '0s')"></xsl:value-of>
      </xsl:when>
      <xsl:otherwise>
       <xsl:value-of select="$publicationYear"></xsl:value-of>
      </xsl:otherwise>
     </xsl:choose>
    </field>
   </xsl:if>
   
   <!-- ++++++++++++++++++++++++++++++++ -->
   <!-- 856 40 and 41 $u links +++++++++++++ -->
   <!-- 856  41  $z R, $3 NR -->
   <!-- only want the first one for the titles listing so put them all in var url and then tokenize, and take first one -->
   <!-- subz can repeat but I couldn't figure out how to test for case insensitive in a sequence (can't use matches, 
    contains doesn't have a case insens flag, )so just looking at first two. that's good enough -->
   <xsl:if test="marc:datafield[@tag=856 and @ind1='4' and (@ind2='0' or @ind2='1')]">
    
    <xsl:variable name="url"> 
     <xsl:for-each select="marc:datafield[@tag=856 and @ind1='4' and (@ind2='0' or @ind2='1')]">
      <!-- sub 3 NR -->
      <xsl:variable name="sub3" select="marc:subfield[@code='3']"></xsl:variable>
      <!-- subz can repeat.  checking first one for 'contents' -->
      <xsl:variable name="subz" select="marc:subfield[@code='z'][1]"></xsl:variable>
      <!-- subz can repeat. checking  second one for 'contents' -->
      <xsl:variable name="subzz" select="marc:subfield[@code='z'][2]"></xsl:variable>
      
      
      
      <xsl:if test="not(matches($sub3, 'contents', 'i'))" >
       <xsl:if test="not(matches($subz, 'contents', 'i'))" >   
        <xsl:if test="not(matches($subzz, 'contents', 'i'))" >   
         <xsl:sequence select="concat(marc:subfield[@code='u'], '|')"></xsl:sequence> 
        </xsl:if>
       </xsl:if>  
      </xsl:if>  
      
      
      
     </xsl:for-each>
    </xsl:variable>  
    
    <!-- break them up with a pipe and take 1st only for titles list field -->
    <xsl:variable name="a" select="tokenize(my:trimTrailingPipe($url), '\|' )"></xsl:variable>
    <field name="linkTitlesList">
     <xsl:value-of select="normalize-space($a[position()=1])"></xsl:value-of>	
    </field>
    
    <!-- we want all of them for the detailed view, so break them back up -->
    <xsl:for-each-group select="$a"
     group-by=".">
     <field name="linkDetailedView">
      <xsl:value-of select="current-grouping-key()"/>
     </field>
    </xsl:for-each-group>
    
   </xsl:if>
   
   <!-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <!-- CALL NO field -->
   <!-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
   <!-- TODO:  need holdings -->
 <!--  <xsl:if test="$holdings/bibs/bib and not($holdings/bibs/bib/holding[$holdIndex]/normalized_call_no = '-')">
    <field name="callNo">
     <xsl:value-of select="$holdings/bibs/bib/holding[$holdIndex]/display_call_no"/>
    </field>
   </xsl:if>  
 -->  
   <!-- Access facet (at a library, or electronic) -->
   <!-- TODO:  requires location -->
<!--   <field name="access_facet">
    <xsl:choose>
     <xsl:when test="matches($voyLocName, 'web')"><xsl:text>Online</xsl:text></xsl:when>
     <xsl:otherwise><xsl:text>At the libraries</xsl:text></xsl:otherwise>
    </xsl:choose>
   </field>
-->   

   <!-- marc record, for searching -->
   <field name="marcrecord">
    <xsl:text>&cdata-start;</xsl:text>
    <record>
     <leader>
      <xsl:value-of select="leader"/>
     </leader>
     <xsl:apply-templates select="marc:controlfield"/>
     <xsl:apply-templates select="marc:datafield"/>
    </record>
    <xsl:text>&cdata-end;</xsl:text>
   </field>
 </doc>
 </xsl:template>

 <xsl:template match="marc:controlfield">
  <xsl:element name="controlfield">
   <xsl:attribute name="tag">
    <xsl:value-of select="@tag"/>
   </xsl:attribute>
   <xsl:apply-templates/>
  </xsl:element>
 </xsl:template>

 <xsl:template match="marc:datafield">
  <xsl:element name="datafield">
   <xsl:attribute name="tag">
    <xsl:value-of select="@tag"/>
   </xsl:attribute>
   <xsl:attribute name="ind1">
    <xsl:value-of select="@ind1"/>
   </xsl:attribute>
   <xsl:attribute name="ind2">
    <xsl:value-of select="@ind2"/>
   </xsl:attribute>
   <xsl:apply-templates select="marc:subfield"/>
  </xsl:element>
 </xsl:template>

 <xsl:template match="marc:subfield">
  <xsl:element name="subfield">
   <xsl:attribute name="code">
    <xsl:value-of select="@code"/>
   </xsl:attribute>
   <xsl:apply-templates/>
  </xsl:element>
 </xsl:template>

 <xsl:template match="*"/>

<!-- .......................................... -->
<!-- .......................................... -->
<!-- ............. FUNCTIONS! ................. -->
<!-- .......................................... -->

 <xsl:function name="my:trimWhitespace">
  <!-- -get rid of multiple spaces anywhere in the string
       -get rid of blank spaces at the start or end of the string
       -get rid of period at the end of the string
       -->
  <xsl:param name="input"/>
  <xsl:value-of
   select="
   my:trimTrailingPeriod(
   replace(replace($input, '\s+', ' '),
    '(^\s+)|(\s+$)', '')
   )
  "
  />
 </xsl:function>

 <xsl:function name="my:trimOnlySpaces">
  <!-- -get rid of multiple spaces anywhere in the string
   -get rid of blank spaces at the start or end of the string	-->
  <xsl:param name="input"/>
  <xsl:value-of select="replace(replace($input, '\s+', ' '),  '(^\s+)|(\s+$)', '') "/>
 </xsl:function>

 <xsl:function name="my:joinAndTrimWhitespace">
  <xsl:param name="sequence"/>
  <xsl:value-of select="my:trimWhitespace(string-join($sequence, ' '))"/>
 </xsl:function>

 <xsl:function name="my:trimTrailingPeriod">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '\.\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimTrailingComma">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, ',\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimTrailingEqual">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '=\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimSemiColon">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '\s*[;]\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimParens">
  <xsl:param name="input"/>
  <xsl:value-of select="replace(replace($input, '\(', ''), '\)', '')"/>
 </xsl:function>

 <!-- gets colon and semicolon -->
 <xsl:function name="my:trimTrailingSemiColon">
  <xsl:param name="input"/>
  <xsl:value-of select="replace(replace($input, ':\s*$', ''), ';\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimTrailingPipe">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '\|+\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimAngleBracket">
  <xsl:param name="input"/>
  <xsl:value-of select="replace(replace($input, '&lt;', ''), '&gt;', '')"/>
 </xsl:function>

  <xsl:function name="my:trimDoubleQuote">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '&quot;', '')"/>
 </xsl:function>

  <xsl:function name="my:trimTrailingColon">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '\s*[:]\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:trimTrailingSlash">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '\s*[/]\s*$', '')"/>
 </xsl:function>

 <xsl:function name="my:convertQuotes">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '&quot;', '''')"/>
 </xsl:function>

 <xsl:function name="my:trimAllButAlphaNum">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '[^a-zA-Z0-9_-]+$', '')"/>
 </xsl:function>

 <!-- Normalize capatilization -->
 <xsl:function name="my:initCap">
  <xsl:param name="input"/>
  <xsl:sequence
   select=" string-join(for $x in tokenize($input, ' ')
       return concat(upper-case(substring($x, 1,1)), substring($x, 2)), ' ')"
  />
 </xsl:function>
</xsl:stylesheet>
