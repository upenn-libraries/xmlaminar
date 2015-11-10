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
   xmlns:integ="http://integrator"
   xmlns:xs="http://www.w3.org/2001/XMLSchema"
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

  <xsl:variable name="locations" select="document('lib/locations.xml')"/>   
  <xsl:variable name="languages" select="document('lib/languages2.xml')"/>   
  <xsl:variable name="class" select="document('lib/ClassOutline.xml')"/> 
  <xsl:variable name="dewclass" select="document('lib/DeweyClass.xml')"/>
 
<!-- this function can be taken out of here when we're not using oxygen, the fn is in dla.upennlib/functions
		be sure to change from my to dla-func  -->
<xsl:function name="my:in">
	<xsl:param name="list"/>
	<xsl:param name="item"/>
	
	<xsl:sequence select="
		if (index-of($list, $item))
		then true()
		else false()
		"/>
</xsl:function>
<!-- end function --> 

 <xsl:function name="my:joinAndTrimWhitespace">
  <xsl:param name="sequence"/>
  <xsl:value-of select="normalize-space(string-join($sequence, ' '))"/>
 </xsl:function>
 
 <xsl:function name="my:stripHyphenISXN">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '-', '')"/>
 </xsl:function>

 <xsl:function name="my:trimLeadingPercen">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '^\s*%\s*', '')"/>
 </xsl:function>

<xsl:function name="my:addTrailingPunct">
	<xsl:param name="input"/>
	<xsl:value-of select="if (ends-with($input, '.')) then ($input)  else (concat($input, '.'))"> </xsl:value-of>
</xsl:function>

 <xsl:function name="my:trimTrailingPeriod">
  <xsl:param name="input"/>
  <xsl:value-of select="if (ends-with($input, 'etc.')) then ($input) else (replace($input, '\.\s*$', ''))"> </xsl:value-of>
  <!--<xsl:value-of select="replace($input, '\.\s*$', '')"/>-->
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

 <xsl:function name="my:convertAmp">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '&amp;', 'and')"/>
 </xsl:function>
 
 <xsl:function name="my:trimAllButAlphaNum">
  <xsl:param name="input"/>
  <xsl:value-of select="replace($input, '[^a-zA-Z0-9_-]+$', '')"/>
 </xsl:function>
 
 <!-- Normalize capitalization -->
 <xsl:function name="my:initCap">
  <xsl:param name="input"/>
  <xsl:sequence
   select=" string-join(for $x in tokenize($input, ' ') 
       return concat(upper-case(substring($x, 1,1)), substring($x, 2)), ' ')"
  />
 </xsl:function>

 <xsl:template match="/">
  <add>
      <xsl:apply-templates select="integ:root/record"/>
  </add>
 </xsl:template>

 <xsl:template match="record">
  <!-- set navigation variables -->
  <xsl:variable name="bibmarc" select="./marc/marc:record"/>
  <xsl:variable name="holdings" select="./holdings/holding"/>

  <doc>
   <!-- DLA-specific format (determines cocoon, etc) -->
   <field name="format">record</field>

   <!-- Record ID -->
   <xsl:variable name="ID" select="$bibmarc/marc:controlfield[@tag='001']"/>
   <field name="id">
    <xsl:value-of select="concat('FRANKLIN_', $ID)"/>
   </field>
   <field name="bibid_field">
    <xsl:value-of select="$ID"/>
   </field>

<!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
<!-- BIB FORMAT facet -->
<!-- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ -->
<xsl:variable name="format" select="substring(($bibmarc/marc:leader), 7,2)"/>
<xsl:variable name="field008" select="$bibmarc/marc:controlfield[@tag='008']"/>
<xsl:variable name="field007" select="$bibmarc/marc:controlfield[@tag='007']"/>
<xsl:variable name="field260press" select="(for $word in $bibmarc/marc:datafield[@tag='260']/marc:subfield[@code='b'] return 
  	if (matches($word, 'press', 'i')) then string('Press' ) else() )">
</xsl:variable>  
<xsl:choose>
    <!-- no matter what the format, if it's Manuscripts location give it format Manuscripts (and nothing else) -->
    <!-- Archives and micro can also only have one format. -->
	<xsl:when test="exists($holdings[matches(location_name, 'manuscripts', 'i')])">
		<field name="format_facet"><xsl:text>Manuscript</xsl:text></field>
		<field name="format_field"><xsl:text>Manuscript</xsl:text></field>
	</xsl:when>
	<xsl:when test="exists($holdings[matches(location_name, 'archives', 'i')])">
		<field name="format_facet"><xsl:text>Archive</xsl:text></field>
		<field name="format_field"><xsl:text>Archive</xsl:text></field>
	</xsl:when>
	<xsl:when test="exists($holdings[matches(location_name, 'micro', 'i')]) or matches($bibmarc/marc:datafield[@tag=245]/marc:subfield[@code='h'], 'micro', 'i')
      	or exists($holdings[matches(display_call_no, 'micro', 'i')])">
			<field name="format_facet"><xsl:text>Microformat</xsl:text></field> 
			<field name="format_field"><xsl:text>Microformat</xsl:text></field> 
	</xsl:when>

	<xsl:otherwise>
		<!-- these next 4 can have this format plus ONE of the formats down farther in the 'choose' --> 
		<xsl:if test="exists($bibmarc/marc:datafield[@tag='502']) and matches($format, 'tm')">
			<field name="format_facet"><xsl:text>Thesis/Dissertation</xsl:text></field>
			<field name="format_field"><xsl:text>Thesis/Dissertation</xsl:text></field>
		</xsl:if>
		<xsl:if test="exists($bibmarc/marc:datafield[@tag='111' or @tag='711'])">
			<field name="format_facet"><xsl:text>Conference/Event</xsl:text></field>
			<field name="format_field"><xsl:text>Conference/Event</xsl:text></field>
		</xsl:if>
 
		<xsl:if test="not(matches(substring($format, 1, 1), '[cdij]')) and 
			 matches(substring($field008, 29, 1), '[fio]') and not(exists($field260press))   ">
			<field name="format_facet"><xsl:text>Government document</xsl:text></field>
			<field name="format_field"><xsl:text>Government document</xsl:text></field>
		</xsl:if>    

		<xsl:if test="matches($format, 'as') and (matches(substring($field008, 22, 1), 'n') or (matches(substring($field008, 23, 1), 'e')))">
			<field name="format_facet"><xsl:text>Newspaper</xsl:text></field>
			<field name="format_field"><xsl:text>Newspaper</xsl:text></field>
		</xsl:if>

		<xsl:choose>
		<xsl:when test="my:in(('aa', 'ac', 'am', 'tm'), $format ) and 
      	( not(matches(string-join($bibmarc/marc:datafield[@tag=245]/marc:subfield[@code='k'], ' '), 'kit', 'i')) )
      	and not(matches($bibmarc/marc:datafield[@tag=245]/marc:subfield[@code='h'], 'micro', 'i'))">
			<field name="format_facet"><xsl:text>Book</xsl:text></field> 
			<field name="format_field"><xsl:text>Book</xsl:text></field> 
		</xsl:when>
		<xsl:when test="my:in(('ca','cb','cd','cm','cs','dm'), $format)">
			<field name="format_facet"><xsl:text>Musical score</xsl:text></field>
			<field name="format_field"><xsl:text>Musical score</xsl:text></field>
		</xsl:when>
		<xsl:when test="starts-with($format, 'e') or matches($format, 'fm')">
			<field name="format_facet"><xsl:text>Map/Atlas</xsl:text></field>
			<field name="format_field"><xsl:text>Map/Atlas</xsl:text></field>
		</xsl:when>
		<xsl:when test="matches($format, 'gm')">
			<xsl:if test="exists($field007[starts-with(., 'v')])">
				<field name="format_facet"><xsl:text>Video</xsl:text></field>
				<field name="format_field"><xsl:text>Video</xsl:text></field>
			</xsl:if>
			<xsl:if test="exists($field007[starts-with(., 'g')])">
				<field name="format_facet"><xsl:text>Projected graphic</xsl:text></field>
				<field name="format_field"><xsl:text>Projected graphic</xsl:text></field>
			</xsl:if>
		</xsl:when>
		<xsl:when test="my:in(('im','jm','jc','jd','js'), $format)">
			<field name="format_facet"><xsl:text>Sound recording</xsl:text></field>
			<field name="format_field"><xsl:text>Sound recording</xsl:text></field>
		</xsl:when>
			<xsl:when test="my:in(('km','kd'), $format)">
			<field name="format_facet"><xsl:text>Image</xsl:text></field>
			<field name="format_field"><xsl:text>Image</xsl:text></field>
		</xsl:when>
		<xsl:when test="matches($format, 'mm')">
			<field name="format_facet"><xsl:text>Datafile</xsl:text></field>
			<field name="format_field"><xsl:text>Datafile</xsl:text></field>
		</xsl:when>
		<xsl:when test="matches($format, 'as') or matches($format, 'gs')">
			<field name="format_facet"><xsl:text>Journal/Periodical</xsl:text></field>
			<field name="format_field"><xsl:text>Journal/Periodical</xsl:text></field>
		</xsl:when>
		<xsl:when test="starts-with($format, 'r')">
			<field name="format_facet"><xsl:text>3D object</xsl:text></field>
			<field name="format_field"><xsl:text>3D object</xsl:text></field>
		</xsl:when>
		<xsl:when test="ends-with($format, 'i')">
			<field name="format_facet"><xsl:text>Database/Website</xsl:text></field>
			<field name="format_field"><xsl:text>Database/Website</xsl:text></field>
		</xsl:when>
<!-- Define "otherwise"... this should never appear with any of the above? -->
<!-- currently, anything that doesn't have a defined bib_format gets "other" (even if has 502, "Thesis" etc) -->
		<xsl:otherwise>
			<field name="format_facet"><xsl:text>Other</xsl:text></field>
			<field name="format_field"><xsl:text>Other</xsl:text></field>
		</xsl:otherwise>
		</xsl:choose>
	</xsl:otherwise>
</xsl:choose>
   
<!-- description_field -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=300]">
	<field name="description_field">
	<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield)"/>
	</field>
</xsl:for-each>
    
<!-- edition_field -->
<xsl:variable name="field_250" select="$bibmarc/marc:datafield[@tag=250][1]"/>
<xsl:if test="exists($field_250)">
	<field name="edition_field">
		<xsl:value-of select="my:joinAndTrimWhitespace($field_250/marc:subfield[@code ne '6' and @code ne '5'])"/>
	</field>
</xsl:if>
   
<!-- publisher_field -->
<xsl:variable name="field_245f" select="$bibmarc/marc:datafield[@tag=245]/marc:subfield[@code='f']"/>
<xsl:variable name="fields_260_261_262" select="$bibmarc/marc:datafield[@tag=260 or @tag=261 or @tag=262]"/>
<!-- this is just the first field -  for brief view. shows 245f if exists, else first 1 of 260 261 262  -->
<xsl:if test="exists($fields_260_261_262) or exists($field_245f)">
	<field name="publisher_field">
		<xsl:value-of select="if ($field_245f) then ($field_245f) else (my:joinAndTrimWhitespace($fields_260_261_262[1]/marc:subfield[@code ne '6' and @code ne '5']))"></xsl:value-of>
	</field>
</xsl:if>

<!-- publisher_search -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=260]/marc:subfield[@code='b']">
	<field name="publisher_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
</xsl:for-each>
<!-- place_of_publication_search  - 260 and 752 -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=260]/marc:subfield[@code='a']">
	<field name="place_of_publication_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
</xsl:for-each>
<!-- 752 still under discussion 
<xsl:for-each select="$bibmarc/marc:datafield[@tag=752]">
	<field name="place_of_publication_search">	
		<xsl:value-of select="./marc:subfield[@code eq 'a' or @code eq 'b' or @code eq 'c' or @code eq 'd' or @code eq 'f' or @code eq 'g' or @code eq 'h']"/>
  	</field>	
</xsl:for-each>  -->
   
   <!-- author_creator_facet -->
   <!-- author_creator_search -->
   <!-- author_creator_sort -->
   <!-- author_creator_field -->
   <!-- Author searches should have fields both in current order, and re-inverted around the comma (e.g. "Joe Bui" and "Bui, Joe" -->
   <!-- 12/12/11 trying to fix author click to search - added trimTrailingComma  and joinAndTrimWhitespace to inverted $a for both author_creator_1 and _2 searches -->
<xsl:variable name="fields_for_author_field" select="$bibmarc/marc:datafield[@tag=100 or @tag=110]"/>
<xsl:variable name="fields_for_author_facet" select="$bibmarc/marc:datafield[@tag=100 or @tag=110 or @tag=111 or @tag=700 or @tag=710 or @tag=711 or @tag=800 or @tag=810 or @tag=811]"/>
<xsl:variable name="fields_for_author2_search" select="$bibmarc/marc:datafield[@tag=100 or @tag=110 or @tag=111 or @tag=400 or @tag=410 or @tag=411 or @tag=700 or @tag=710 or @tag=711 or @tag=800 or @tag=810 or @tag=811]"/>

<xsl:for-each select="$fields_for_author_field">
	<field name="author_creator_field">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '6' and @code ne '4' and @code ne '5'] )"/>
	</field>
</xsl:for-each>
  	
<xsl:for-each select="$fields_for_author_facet">
	<field name="author_creator_facet">
		<xsl:value-of select="my:trimTrailingPeriod(my:convertAmp(my:joinAndTrimWhitespace(./marc:subfield[@code eq 'a' or @code eq 'b' or @code eq 'c' or @code eq 'd'])))"/>
	</field>
	<field name="author_creator_1_search">
		<xsl:value-of select="my:joinAndTrimWhitespace((./marc:subfield[@code ne '6' and @code ne '4']))"/>
	</field>
	<field name="author_creator_1_search">
	<xsl:for-each select="./marc:subfield[@code='a']">
		<xsl:value-of select="my:joinAndTrimWhitespace(my:trimTrailingComma(substring-after(., ', ')))"/><xsl:text> </xsl:text><xsl:value-of select="substring-before(., ',')"/>
	</xsl:for-each>
	<xsl:text> </xsl:text>
		<xsl:value-of select="my:joinAndTrimWhitespace((./marc:subfield[@code ne 'a' and @code ne '6' and @code ne '4']))"/>
	</field>
</xsl:for-each>

<xsl:if test="$fields_for_author_field">
	<field name="author_creator_sort">
		<xsl:value-of select="my:joinAndTrimWhitespace($fields_for_author_field[1]/marc:subfield[@code ne '6' and @code ne '4'])"/>
	</field>
</xsl:if>

<xsl:for-each select="$fields_for_author2_search">
	<field name="author_creator_2_search">
		<xsl:value-of select="my:joinAndTrimWhitespace((./marc:subfield[@code ne '6' and @code ne 't']))"/>
	</field>
	<field name="author_creator_2_search">
	<xsl:for-each select="./marc:subfield[@code='a']">
		<xsl:value-of select="my:joinAndTrimWhitespace(my:trimTrailingComma(substring-after(., ',')))"/><xsl:text> </xsl:text><xsl:value-of select="substring-before(., ',')"/>
	</xsl:for-each>
	<xsl:text> </xsl:text>
		<xsl:value-of select="my:joinAndTrimWhitespace((./marc:subfield[@code ne 'a' and @code ne '6' and @code ne 't']))"/>
	</field>
</xsl:for-each>

<!-- corporate_author_search -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=110 or @tag=710 or @tag=810]">
	<field name="corporate_author_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code='a' or @code='b' or @code='c' or @code='d'])"/>
	</field>
</xsl:for-each>
   
<!-- conference_field -->
<!-- NOTE: different for detail display -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=111]">
	<field name="conference_field">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '6' and @code ne '5'])"/>
	</field>
</xsl:for-each>
   <!-- conference_search -->
  	<xsl:for-each select="$bibmarc/marc:datafield[@tag=111 or @tag=711 or @tag=811]">
	<field name="conference_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code='a' or @code='b' or @code='c' or @code='d'])"/>
	</field>
</xsl:for-each>

    
<!-- series_field -->
<!-- series_search -->
<xsl:variable name="fields_8xx_for_series" 
               select="$bibmarc/marc:datafield[@tag=800 or @tag=810 or @tag=811 or @tag=830]"/>
<xsl:variable name="fields_4xx_for_series" 
 	select="$bibmarc/marc:datafield[@tag=400 or @tag=410 or @tag=411 or @tag=440 or @tag=490]"/>
  
<xsl:if test="$fields_8xx_for_series or $fields_4xx_for_series">
	<field name="series_field">
		<xsl:value-of select="my:joinAndTrimWhitespace( if ($fields_8xx_for_series) 
			then $fields_8xx_for_series[1]/marc:subfield[@code ne '6' and @code ne '5']
						else (if ($fields_4xx_for_series) 
						then $fields_4xx_for_series[1]/marc:subfield[@code ne '6' and @code ne '5']
								else () ) )"/>
	</field>
	<field name="series_search">
		<xsl:value-of select="my:joinAndTrimWhitespace( if ($fields_8xx_for_series) 
			then $fields_8xx_for_series[1]/marc:subfield[@code ne '6']
			else (if ($fields_4xx_for_series) 
			then $fields_4xx_for_series[1]/marc:subfield[@code ne '6']
			else () ) )"/>
	</field>
  </xsl:if>

   
<!-- contained_in_field --> 
<xsl:for-each select="$bibmarc/marc:datafield[@tag=773]">
	<field name="contained_in_field">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '6' and @code ne '5'])"/>
	</field>
</xsl:for-each>

<!-- isxn_search -->
<!-- pubnum_search -->
<!-- TODO: do isbn 10/13 conversion -->
<!-- isbns get a _field and _search -->
<!-- one with hyphen and one without -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=020]/marc:subfield[@code='a' or @code='z']">
	<field name="isbn_field">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
	<field name="isxn_custom_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(my:stripHyphenISXN(.))"/>
	</field>
	<field name="isxn_custom_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
</xsl:for-each>
<!-- issns, just a search -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=022]/marc:subfield[@code='a' or @code='z'] ">
	<field name="isxn_custom_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(my:stripHyphenISXN(.))"/>
	</field>
	<field name="isxn_custom_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
</xsl:for-each>
<!-- pubnums, will not get same cleanup for search, since they're not standardized -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=024 or @tag=028]">
	<field name="pubnum_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code='a'])"/>
	</field>
</xsl:for-each>
   
<!-- TITLE -->
<!-- title for sort -->
<!-- strip leading [ from sort field - both a and k (replace(  field ,'^\[',''))"/>  -->
<xsl:variable name="first_245" select="$bibmarc/marc:datafield[@tag=245][1]"/>
<xsl:variable name="title_offset" select="if ($first_245/@ind2 eq ' ') then 0 else $first_245/@ind2 " />
<field name="title_sort">
<xsl:choose>
	<xsl:when test="$title_offset &gt; 0 and $title_offset &lt; 10">
		<!-- 245 $a minus the leading article -->
		<xsl:value-of select="substring($first_245/marc:subfield[@code='a'], $title_offset + 1)"/>
	</xsl:when>
	<xsl:otherwise>
	<!-- if there's no article (245 2nd ind) just take $a. if no $a, use $k. remove [ if at start of $a or $k. 
       haven't accounted for if $k has 245 2nd ind. It shouldn't, but some do anyway --> 
	<xsl:value-of select="if ($first_245/marc:subfield[@code='a'])
		then replace($first_245/marc:subfield[@code='a'],'^\[','')
		else (replace(string-join($first_245/marc:subfield[@code='k'], ' '),'^\[','') )"/>
	</xsl:otherwise>
</xsl:choose>
<xsl:text> </xsl:text>
<xsl:value-of select="$first_245/marc:subfield[@code='b']"/>
<xsl:text> </xsl:text>
<xsl:value-of select="$first_245/marc:subfield[@code='n']"/>
<xsl:text> </xsl:text>
<xsl:value-of select="$first_245/marc:subfield[@code='p']"/>
</field>

<!-- title_field -->
<!-- title with most punct stripped out. = and : at end of $a $k $h but no other punct -->
<!-- if a k or h ends with = or :, put that punct in bwtn  a and b in title_nopunct_field -->
<!-- k can repeat, so string join in case there are multiples -->
<xsl:variable name="titlewithslash" select="if ($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='a'])
		then $bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='a']
		else ( string-join($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='k'], ' ' ))"></xsl:variable>
<xsl:variable name="title_ak" select="my:trimTrailingComma(my:joinAndTrimWhitespace(my:trimTrailingSlash($titlewithslash)))"/> 

<!-- get the punct at end, if any. substring starts counting at zero, so $len gets the last char, don't need $len-1 -->
<xsl:variable name="len" select="string-length($title_ak)"></xsl:variable>
<xsl:variable name="apunct" select="substring($title_ak, ($len cast as xs:double), 1)"></xsl:variable> 

<xsl:variable name="len" select="string-length(my:joinAndTrimWhitespace($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='h']))"></xsl:variable>
<xsl:variable name="hpunct" select="substring(my:joinAndTrimWhitespace($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='h']), ($len cast as xs:double), 1)"></xsl:variable>

<xsl:variable name="punct" select="if ( matches($apunct, '=') or matches($hpunct, '=') ) then '=' else(
		if ( matches($apunct, ':') or matches($hpunct, ':') ) then ':' else() )"></xsl:variable> 

<!-- 245 a punct b n p  for brief view/search results -->
<field name="title_field">
	<xsl:value-of select="my:joinAndTrimWhitespace(concat(my:trimTrailingColon(my:trimTrailingEqual($title_ak)), ' ', $punct, ' ', 
		my:trimTrailingSlash($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='b'][1]), ' ', 
		my:trimTrailingSlash($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='n'][1]), ' ', 
		my:trimTrailingSlash($bibmarc/marc:datafield[@tag=245][1]/marc:subfield[@code='p'][1])))"/>	
</field> 



<!-- title_search -->
<!-- journal_title_search => same as title, but only include if bib_format is 'serial' -->
<!--  exclude 245 $c  and $6 also exclude $h - we think that will help relevance and put elec version at top of list  -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=245]">
	<field name="title_1_search">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'c' and @code ne '6' and @code ne 'h'])"/>
	</field>
	<xsl:if test="ends-with($format, 's')">
		<field name="journal_title_1_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'c' and @code ne '6' and @code ne 'h'])"/>
		</field>
	</xsl:if>
</xsl:for-each>

<!-- 773, 774, 780, 785 have subf restrictions -->
<xsl:variable name="fields_for_title_2_search_main" 
    select="$bibmarc/marc:datafield[@tag=130 
                                 or @tag=240 or @tag=245 or @tag=246 or @tag=247
                                 or @tag=440 or @tag=490
                                 or @tag=730 or @tag=740 
                                 or @tag=830 or @tag=840]"/>
<xsl:variable name="fields_for_title_2_search_aux" 
	select="$bibmarc/marc:datafield[@tag=773 or @tag=774 or @tag=780 or @tag=785]"/>
  	
<xsl:for-each select="$fields_for_title_2_search_main">
	<field name="title_2_search">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'c' and @code ne '6'])"/>
	</field>
	<xsl:if test="ends-with($format, 's')">
		<field name="journal_title_2_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'c' and @code ne '6'])"/>
		</field>
	</xsl:if>
</xsl:for-each>
<xsl:for-each select="$fields_for_title_2_search_aux">
	<field name="title_2_search">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code eq 's' or @code eq 't'])"/>
	</field>
	<xsl:if test="ends-with($format, 's')">
		<field name="journal_title_2_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code eq 's' and @code eq 't'])"/>
		</field>
	</xsl:if>
</xsl:for-each>
   
<!-- Standardized title -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=130 or @tag=240]">
	<field name="standardized_title_field">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '6' and @code ne '5'])"/>
	</field>
</xsl:for-each>
   
<!-- 880 crap -->
<!-- 100, 110 and 245 are good to go for brief  -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag='880']">
	<xsl:variable name="sub6"><xsl:value-of select="substring(./marc:subfield[@code='6'], 1,3)"/></xsl:variable>  		
	<xsl:choose>
		<!-- 100 and 110 all subf  for brief view-->
		<xsl:when test="my:in(('100', '110'), $sub6)">
			<field name="author_880_field">
				<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code != '6' and @code != '4' and @code ne '5'])"/>		
			</field>
			<field name="author_creator_1_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code != '6' and @code != 't'])"/>
			</field>
		</xsl:when>
		<!-- 245 a k b n p  for brief view-->
		<xsl:when test="matches($sub6, '245')">
		<!-- title with most punct stripped out. = and : at end of $a $k $h but no other punct -->
		<!-- if a k or h ends with = or :, put that punct in bwtn  a and b in title_nopunct_field -->
		<xsl:variable name="titlewithslash" select="if (./marc:subfield[@code='a'][1])
			then ./marc:subfield[@code='a'][1]
			else (string-join(./marc:subfield[@code='k'], ' '))"></xsl:variable>
		<xsl:variable name="title_ak" select="my:trimTrailingComma(my:joinAndTrimWhitespace(my:trimTrailingSlash($titlewithslash)))"/>

		<!-- get the punct at end, if any. substring starts counting at zero, so $len gets the last char, don't need $len-1 -->
		<xsl:variable name="len" select="string-length($title_ak)"></xsl:variable>
		<xsl:variable name="apunct" select="substring($title_ak, ($len cast as xs:double), 1)"></xsl:variable> 

		<xsl:variable name="len" select="string-length(my:joinAndTrimWhitespace(./marc:subfield[@code='h']))"></xsl:variable>
		<xsl:variable name="hpunct" select="substring(my:joinAndTrimWhitespace(./marc:subfield[@code='h']), ($len cast as xs:double), 1)"></xsl:variable>

		<xsl:variable name="punct" select="if ( matches($apunct, '=') or matches($hpunct, '=') ) then '=' else(
			if ( matches($apunct, ':') or matches($hpunct, ':') ) then ':' else() )"></xsl:variable> 

		<!-- 880 a punct b n p  for brief view/search results -->
		<field name="title_880_field">
		<xsl:value-of select="my:joinAndTrimWhitespace(concat(my:trimTrailingColon(my:trimTrailingEqual($title_ak)), ' ', $punct, ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='b'][1]), ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='n'][1]), ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='p'][1])))"/>	
		</field>
		<!-- get the 880 for title in the title_1_search -->
		<field name="title_1_search">
		<xsl:value-of select="my:joinAndTrimWhitespace(concat(my:trimTrailingColon(my:trimTrailingEqual($title_ak)), ' ', $punct, ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='b'][1]), ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='n'][1]), ' ', 
		my:trimTrailingSlash(./marc:subfield[@code='p'][1])))"/>
		</field>
		</xsl:when>

		<!-- 880 fields into search fields, for click-to-search on the detailed view-->
		<!-- these tags have diff subf restrictions than 773 774 780 785 (below) -->
		<xsl:when test="my:in(('130', '240', '245', '246','247','440', '490', '730','740','830','840'), $sub6)">
			<field name="title_2_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code != '6' and @code ne 'c'])"/>
			</field>
		</xsl:when>
		<xsl:when test="my:in(('773', '774', '780', '785'), $sub6)">
			<field name="title_2_search">
				<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code eq 's' or @code eq 't'])"/>
			</field>
		</xsl:when>
		<xsl:when test="my:in(('100', '110','111','400','410','411','700','710','711','800','810','811'), $sub6)">
			<field name="author_creator_2_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code != '6' and @code ne 't'])"/>
			</field>
			<!-- invert first name last name -->
			<field name="author_creator_2_search">
			<xsl:for-each select="./marc:subfield[@code='a']">
				<xsl:value-of select="my:joinAndTrimWhitespace(my:trimTrailingComma(substring-after(., ',')))"/><xsl:text> </xsl:text><xsl:value-of select="substring-before(., ',')"/>
			</xsl:for-each>
			<xsl:text> </xsl:text>
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'a' and @code ne '6' and @code ne 't'])"/>
			</field>
		</xsl:when>
		<!-- get the PRO and CHR out or the click to searches for Provenance and Chronology on detailed pg don't work -->
		<xsl:when test="( my:in(('541', '561', '600','610','611','630','650','651','653'), $sub6) or starts-with($sub6, '69') )">
			<field name="subject_search">
			<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '6'])"/>
			</field>
		</xsl:when>
	</xsl:choose>
</xsl:for-each>
<!-- end 880 -->

<!-- language_facet -->
<xsl:variable name="lang008" select="substring($bibmarc/marc:controlfield[@tag=008],36,3)"></xsl:variable>
<xsl:variable name="language" select="$languages/languages/lang[@code=$lang008]"/>
<xsl:if test="$language != ''">
	<field name="language_facet">
	<xsl:value-of select="$language"></xsl:value-of>
	</field>
	<field name="language_search">
	<xsl:value-of select="$language"></xsl:value-of>
	</field>
</xsl:if>
   
<!-- *** PUBLICATION *** -->
<!-- publication_date_field - put on the end of publisher string in brief view-->
<xsl:variable name="currYear" select="format-date(current-date(), '[Y]') cast as xs:integer"></xsl:variable>
<xsl:variable name="publicationYear" select="substring($bibmarc/marc:controlfield[@tag=008],8,4)"></xsl:variable>
<xsl:if test="$publicationYear ne '    '">
	<field name="publication_date_field">
	<xsl:value-of select="$publicationYear"/>
	</field>
</xsl:if>
<!-- publication_date_facet -->
<!-- from Katia: "like NB+ but all decades (2011 will stand alone until more years come into the decade) -->
<xsl:if test="not(matches($publicationYear, '^[ux9]'))">
<xsl:variable name="pubYearDigits" select="replace($publicationYear, '\D', '0')"/>
<xsl:if test="matches($pubYearDigits, '^[1-9][0-9]') and (($currYear + 15) &gt; $pubYearDigits cast as xs:integer)">
	<field name="publication_date_facet">
	<xsl:value-of select="concat(substring($pubYearDigits, 1,3), '0s')"></xsl:value-of>
	</field>
</xsl:if>
<field name="publication_date_sort">
<xsl:value-of select="$pubYearDigits"/>
</field>
    <xsl:if test="normalize-space($pubYearDigits) != ''">
    <field name="publication_date_custom_search">
    <xsl:value-of select="$pubYearDigits"/>
    </field>
    </xsl:if>
</xsl:if>
   
<!-- ****************************************** -->
<!-- *** Holdings *** -->
<!-- ****************************************** -->
   
<!-- *** Locations *** -->
<!-- uses xml lookup -->
<xsl:for-each select="$holdings">

<!-- if count of items is 0, the loc is the holding loc -->
<xsl:if test="count(./items/item) = 0">
<xsl:variable name="loc" select="hold_location_code"/>
	<!-- this is what to pull out in a template if possible -->
	<xsl:variable name="loclookup" select="$locations/locations/location[@location_code=$loc]"/>
	<!-- specific_location_facet -->
	<xsl:for-each select="$loclookup/specific_location">	
		<field name="specific_location_facet">
		<xsl:value-of select="my:convertAmp(.)"></xsl:value-of>
		</field>
	</xsl:for-each>
	<!-- library_facet i.e. location (larger) -->
	<xsl:for-each select="$loclookup/library">
		<field name="library_facet">
		<xsl:value-of select="my:convertAmp(.)"></xsl:value-of>
		</field>
	</xsl:for-each>
	<!-- display_location_field -->
	<field name="display_location_field">
	<xsl:choose>
		<xsl:when test="exists($loclookup/display)">
			<xsl:value-of select="$loclookup/display"></xsl:value-of>
		</xsl:when>
		<xsl:otherwise>
			<!-- <xsl:value-of select="location_display_name"></xsl:value-of> -->
			<!-- use the loc code here, if this displays it will be more obvious (means the locations.xml is missing this loc) -->
			<xsl:value-of select="$loc"></xsl:value-of> 
		</xsl:otherwise>
	</xsl:choose>
	</field>
	<!-- access_facet -->
	<field name="access_facet">
	<xsl:choose>
		<xsl:when test="contains($loc, 'web') and $bibmarc/marc:datafield[@tag=856 and @ind1 = '4']/marc:subfield[@code='u']">
			<xsl:text>Online</xsl:text>
		</xsl:when>
		<xsl:otherwise><xsl:text>At the library</xsl:text></xsl:otherwise>
	</xsl:choose>
	</field>
	<!-- end template -->

</xsl:if>

<xsl:if test="count(./items/item) != 0">
<xsl:for-each select="./items/item">
<xsl:variable name="loc" select="
		if (temp_location_code != '')
		then temp_location_code
		else (perm_location_code)
		"/>

	<xsl:variable name="loclookup" select="$locations/locations/location[@location_code=$loc]"/>
	<xsl:for-each select="$loclookup/specific_location">
		<!-- specific_location_facet -->
		<field name="specific_location_facet">
		<xsl:value-of select="my:convertAmp(.)"></xsl:value-of>
		</field>
	</xsl:for-each>
	<!-- library_facet i.e. location (larger) -->
	<xsl:for-each select="$loclookup/library">
		<field name="library_facet">
		<xsl:value-of select="my:convertAmp(.)"></xsl:value-of>
		</field>
	</xsl:for-each>
	<!-- display_location_field -->
	<field name="display_location_field">
	<xsl:choose>
		<xsl:when test="exists($loclookup/display)">
			<xsl:value-of select="$loclookup/display"></xsl:value-of>
		</xsl:when>
		<xsl:otherwise>
			<!-- <xsl:value-of select="location_display_name"></xsl:value-of> -->
			<!-- use the loc code here, if this displays it will be more obvious (means the locations.xml is missing this loc) -->
			<xsl:value-of select="$loc"></xsl:value-of> 
		</xsl:otherwise>
	</xsl:choose>
	</field>
	<!-- access_facet -->
	<field name="access_facet">
            <xsl:choose>
		<xsl:when test="contains($loc, 'web') and $bibmarc/marc:datafield[@tag=856 and @ind1 = '4']/marc:subfield[@code='u']  ">
			<xsl:text>Online</xsl:text>
		</xsl:when>
		<xsl:otherwise><xsl:text>At the library</xsl:text></xsl:otherwise>
	</xsl:choose>
	</field>
</xsl:for-each>
</xsl:if>



	<!-- call_number_search -->
	<field name="call_number_search">
	<xsl:value-of select="display_call_no"/>
	</field>

	<!-- classification_facet -->
	<!-- Broad Lib of Congress or Dewey class. First letter/digit of call number -->
	<xsl:variable name="firstLetter" select="substring(normalized_call_no, 1,1)"></xsl:variable>
	<!-- LC call numbers -->
	<xsl:if test="call_no_type eq '0' ">
		<field name="classification_facet">
		<xsl:value-of select="concat($firstLetter, ' - ', my:convertAmp($class/list/class[@value=$firstLetter]))"></xsl:value-of>
		</field>
	</xsl:if>
	<!-- Dewey call numbers -->
	<xsl:if test="(call_no_type eq '1') and matches(substring(normalized_call_no, 1, 3), '\d\d\d')">
		<field name="classification_facet">
		<xsl:value-of select="concat($firstLetter, '00 - ', my:convertAmp($dewclass/list/class[@value=$firstLetter]))"></xsl:value-of>
		</field>
	 </xsl:if> 



</xsl:for-each> <!-- end if holdings -->
<!-- *** end HOLDINGS loop *** -->

<!-- recently_added_sort -->
<!-- most recent holding create date -->
<xsl:if test="$holdings">
	<field name="recently_added_sort">
	<xsl:variable name="hldg_create_date" select="$holdings/hold_create_date/replace(text(), '\D', '')"/>
	<xsl:value-of select="max($hldg_create_date)"></xsl:value-of>
	</field>
</xsl:if>
   
<!-- contents_note_search -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=505]">
	<field name="contents_note_search">
	<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield)"/>
	</field>
</xsl:for-each>

<!-- FORM/GENRE  -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=655]">
	<!-- make the search for all 655s? -->
	<field name="genre_search">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '2' and @code ne '6'])"/>
	</field>
	
	<!-- make facet for manuscripts -->
	<xsl:if test="(exists($holdings[matches(location_name, 'manuscripts', 'i')]))">
		<field name="genre_facet">
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '2' and @code ne '6' and @code ne '5'])"/>
		</field>
	</xsl:if>  
	
	<!-- make facet for videos -->
	<xsl:if test="matches($format, 'gm')">
		<xsl:if test="exists($field007[starts-with(., 'v')])">   
			<field name="genre_facet">
				<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne '2' and @code ne '6' and @code ne '5'])"/>
			</field>
		</xsl:if>
	</xsl:if>   
			
</xsl:for-each> 


<!-- subject_facet -->
<!-- subject_search -->
<!-- from Katia: one facet with both "6xx all marc:subfields; 6xx $a" values -->
<!-- Exclude "PRO" and "CHR" 6xx from subject facet (but not search, will be click-to-search) -->
<xsl:variable name="fields_65x" select="$bibmarc/marc:datafield[@tag=600 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
		or @tag=610 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
		or @tag=611 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
		or @tag=630 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
		or @tag=650 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2')
		or @tag=651 and (@ind2 = '4' or @ind2 = '0' or @ind2 = '2') ]"/>
<xsl:for-each select="$fields_65x">
	<xsl:variable name="subj">
		<xsl:for-each select="./marc:subfield[@code='a']">
			<xsl:if test="not(starts-with(., 'PRO')) and not(starts-with(., 'CHR'))
			and not(starts-with(., '%PRO')) and not(starts-with(., '%CHR') )">
				<xsl:value-of select="."></xsl:value-of>
			</xsl:if>
		</xsl:for-each>
		<xsl:text> </xsl:text>
		<xsl:value-of select="my:joinAndTrimWhitespace(./marc:subfield[@code ne 'a'  and @code ne '6' and @code ne '5'])"/>
	</xsl:variable>
	
	<xsl:if test="$subj > ' ' ">
		<field name="subject_facet">
		<xsl:value-of select="my:trimTrailingPeriod($subj)"/>
		</field>
	</xsl:if>

	<!-- just the sub a -->
	<xsl:if test="exists(./marc:subfield[@code='a']) and exists(./marc:subfield[@code ne 'a'])">
		<xsl:if test="not(starts-with(., 'PRO')) and not(starts-with(., 'CHR'))
		and not(starts-with(., '%PRO')) and not(starts-with(., '%CHR') )">
			<field name="subject_facet">
			<xsl:value-of select="./marc:subfield[@code='a']"></xsl:value-of>
			</field>
		</xsl:if>
	</xsl:if>
	
</xsl:for-each>
<!-- remove CHR and PRO, also a trailing ? -->
<xsl:for-each select="$bibmarc/marc:datafield[@tag=541 or @tag=561 or @tag=600 or @tag=610  
                                              	or @tag=611 or @tag=630 or @tag=650 or @tag=651 or @tag=653
                                            	or starts-with(@tag, '69')]">
	<xsl:variable name="subj">
		<xsl:for-each select="./marc:subfield[@code='a']">
			<xsl:value-of select="replace(replace(replace(replace(replace(. ,'^%PRO',''), '^PRO', ''), '^%CHR', ''), '^CHR', ''), '\?$', '') "></xsl:value-of>
		</xsl:for-each>
		<xsl:text> </xsl:text>
		<xsl:value-of select="(./marc:subfield[@code ne 'a' and @code ne '6' and @code ne '5'])"/>
	</xsl:variable>
 	<xsl:if test="$subj > ' ' ">
		<field name="subject_search">
			<xsl:value-of select="my:joinAndTrimWhitespace($subj)"/>
		</field>
 	</xsl:if>   
</xsl:for-each>
   
<!-- Full Text Display (URL for full text)-->
  	<xsl:for-each select="$bibmarc/marc:datafield[@tag=856 and @ind1 = '4' and (@ind2 = '0' or @ind2 = '1')]">
	<xsl:variable name="url" select="my:joinAndTrimWhitespace(replace((./marc:subfield[@code='u'][1]), ' target=_blank', ''))"></xsl:variable> 
  		
  		<xsl:variable name="linktext_3" select="my:convertAmp((./marc:subfield[@code = '3'][1]))"/>
		<xsl:variable name="linktext_zy" select="my:joinAndTrimWhitespace(my:convertAmp( if (./marc:subfield[@code = 'z'])
  				then (./marc:subfield[@code = 'z'][1])
  				else ( 
  				if (./marc:subfield[@code = 'y']) 
  				then (./marc:subfield[@code = 'y'][1]) 
  				else() ) ))">
		</xsl:variable>
  	<!-- chopped off target= blank when making var url so don't need to do it here --> 
  		<xsl:variable name="link_label">
  			<xsl:value-of select="my:joinAndTrimWhitespace(
  					if ( $linktext_3 or $linktext_zy )
  					then string-join(($linktext_3, $linktext_zy), ' ')
  					else ($url)
  					)"></xsl:value-of>
  		</xsl:variable>
  		  		
  		<field name="full_text_link">
                   <a href="{$url}"><xsl:value-of select="$link_label"/></a>
                </field>
  	</xsl:for-each>




<!-- Restrictions on Use, display on detailed view upper right corner - but without label 
 12/13/11 I think this is entirely dealt with in detailed.xsl and we don't need this field -->
<!-- <xsl:for-each select="$bibmarc/marc:datafield[@tag=506]">
	<field name="restrictions_on_use_field">
	<xsl:value-of select="my:joinAndTrimWhitespace(.)"/>
	</field>
</xsl:for-each>
 -->
  	
  	
<!-- marc record, for searching -->
<field name="marcrecord">
<record>
<xsl:copy-of select="$bibmarc/*"></xsl:copy-of>
</record>
</field>
  
</doc>
  
</xsl:template>

<xsl:template match="*"/>

</xsl:stylesheet>
