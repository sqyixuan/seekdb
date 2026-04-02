delimiter //;
create or replace package pdms.DM_PKG_UTILS is

  type cursorType is ref cursor;

  procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2);

  procedure foundIn(src in varchar2, str in varchar2, founded out boolean);

  procedure append(src in varchar2, str in varchar2, results out varchar2);

  procedure remove(src in varchar2, str in varchar2, results out varchar2);

  procedure stringToArray1(src in varchar2, results out NT, length out number);

  procedure stringToArray2(src  in varchar2, results out VT, length out number);

  procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number);

  procedure stringToArray4(src  in varchar2, pattern in varchar2, results out VT, length out number);

  function padding(src in varchar2, char in varchar2, len in number, type in varchar2) return varchar2;

  function getRegionIdByDataSeg(strDataSeg in varchar2) return varchar2;

  function getRegionIdByAttrId(strAttrId in number) return varchar2;

end DM_PKG_UTILS;
//

create or replace package body pdms.DM_PKG_UTILS is
  procedure tokenizer(iStart in number, sPattern in varchar2, sBuffer in varchar2, sResult out varchar2, sNextBuffer out varchar2)
  is
    nPos1 number;
    begin
    nPos1 := instr(sBuffer, sPattern, iStart);
    if (nPos1 = 0 and sBuffer is not null) then
      sResult := sBuffer;
      sNextBuffer := null;
    else
      sResult := substr(sBuffer, 1, nPos1-1);
      sNextBuffer := substr(sBuffer, nPos1+1);
    end if;
    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure tokenizer:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end tokenizer;

  procedure foundIn(src in varchar2, str in varchar2, founded out boolean)
  is
    temp_src varchar2(32767) := '';
    temp_str varchar2(32767) := '';
    temp_nextstr varchar2(32767) := ' ';
  begin
    if src is null or str is null then
      founded := false;
    else
      founded := false;
      temp_src := src;
      while(temp_nextstr is not null) loop
        tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
        if temp_str = str then
          founded := true;
          return;
        end if;
        temp_src := temp_nextstr;
      end loop;
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure foundin:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end foundin;

  procedure append(src in varchar2, str in varchar2, results out varchar2)
  is
    temp_founded boolean := false;
  begin
    if src is null then
      if str is null then
        results := '';
      else
        results := str;
      end if;
    else
      if str is null then
        results := src;
      else
        foundin(src, str, temp_founded);
        if temp_founded then
          results := src;
        else
          results := src || ',' || str;
        end if;
      end if;
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure append:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end append;

  procedure remove(src in varchar2, str in varchar2, results out varchar2)
  is
    temp_src varchar2(32767) := '';
    temp_str varchar2(32767) := '';
    temp_nextstr varchar2(32767) := ' ';
  begin
    if src is null or str is null then
      results := '';
    else
      temp_src := src;
      while(temp_nextstr is not null) loop
        tokenizer(1, ',', temp_src, temp_str, temp_nextstr);
        if temp_str != str then
          results := results || temp_str || ',';
        end if;
        temp_src := temp_nextstr;
      end loop;
      results := substr(results, 0, length(results)-1);
    end if;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure remove:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end remove;

  procedure stringToArray1(src in varchar2, results out NT, length out number)
  is
  begin
    stringToArray3(src, ',', results, length);
  end stringtoarray1;

  procedure stringToArray2(src in varchar2, results out VT, length out number)
  is
  begin
    stringToArray4(src, ',', results, length);
  end stringtoarray2;

  procedure stringToArray3(src in varchar2, pattern in varchar2, results out NT, length out number)
  is
    temp_src varchar2(32767) := src;
    temp_str varchar2(32767) := null;
    temp_next varchar2(32767) := ' ';
    temp_nt NT := NT();
    i number := 0;
  begin
    while (temp_next is not null) loop
      tokenizer(1, pattern, temp_src, temp_str, temp_next);
      i := i+1;
      length := i;
      temp_nt.extend();
      temp_nt(temp_nt.count) := temp_str;
      temp_src := temp_next;
    end loop;
    results := temp_nt;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end stringToArray3;

  procedure stringToArray4(src in varchar2, pattern in varchar2, results out VT, length out number)
  is
    temp_src varchar2(32767) := src;
    temp_str varchar2(32767) := null;
    temp_next varchar2(32767) := ' ';
    temp_vt VT := VT();
    i number := 0;
  begin
    while (temp_next is not null) loop
      tokenizer(1, pattern, temp_src, temp_str, temp_next);
      i := i+1;
      length := i;
      temp_vt.extend();
      temp_vt(temp_vt.count) := temp_str;
      temp_src := temp_next;
    end loop;
    results := temp_vt;

    exception when others then
      dbms_output.put_line('ERROR: an error occurred in procedure stringtoarray:');
      dbms_output.put_line('       error code: ' || to_char(sqlcode));
      dbms_output.put_line('       error message: ' || sqlerrm);
  end stringtoarray4;

  function padding(src in varchar2, char in varchar2, len in number, type in varchar2) return varchar2
  is
    temp_length number := 0;
    temp_str varchar2(50) := null;
    temp_type varchar2(1) := 'L';
    temp_i number := 0;
    temp_results varchar2(255) := null;
  begin
    if length(src) >= len then
      temp_results := src;
    else
      temp_type := Upper(type);
      temp_length := len - length(src);
      while temp_i < temp_length loop
        temp_str := temp_str || char;
        temp_i := temp_i + 1;
      end loop;

      if temp_type = 'L' then
        temp_results := temp_str || src;
      end if;
      if temp_type = 'R' then
        temp_results := src || temp_str;
      end if;
    end if;

    return temp_results;
  end padding;

function getRegionIdByDataSeg(strDataSeg in varchar2) return varchar2
	is
		temp_flag number := 0;
		temp_attr_name varchar2(24) := null;
		temp_parent_id number := 0;
		temp_attr_level number := 0;
		temp_region_Id number := -1;
	begin
		select count(*) into temp_flag from DM_DATA_SEG where DATA_SEG=strDataSeg;
		if temp_flag < 1 then
			return temp_region_Id;
		end if;

		select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=(select ATTR_ID from DM_DATA_SEG where DATA_SEG=strDataSeg);
		if temp_flag < 1 then
			return temp_region_Id;
		end if;
        
		-- 此段代码有问题，拆分为2段代码
		select ATTR_NAME, PARENT_ID, ATTR_LEVEL into  temp_attr_name, temp_parent_id,temp_attr_level
			from DM_HLR_LINK where ATTR_ID=(select ATTR_ID from DM_DATA_SEG where DATA_SEG=strDataSeg);
		
		select count(*) into temp_flag
			from DM_HLR_LINK where ATTR_ID=(select ATTR_ID from DM_DATA_SEG where DATA_SEG=strDataSeg);

		if temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_1 and temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_2 then
			return temp_region_Id;
		end if;

		if temp_attr_level=DM_PKG_CONST.ATTR_LEVEL_2 then
			select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=temp_parent_id;
			if temp_flag < 1 then
				return temp_region_Id;
			end if;
			select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=temp_parent_id;
		end if;
		select count(*) into temp_flag from DM_REGION where REGION_NAME=temp_attr_name;
		if temp_flag < 1 then
				return temp_region_Id;
		end if;
		select REGION_ID into temp_region_Id from DM_REGION where REGION_NAME=temp_attr_name;

		return temp_region_Id;
	end getRegionIdByDataSeg;

  function getRegionIdByAttrId(strAttrId in number) return varchar2
  is
    temp_flag number := -1;
    temp_attr_name varchar2(24) := null;
    temp_parent_id number := 0;
    temp_attr_level number := 0;
    temp_region_Id number := -1;
  begin
    select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=strAttrId;
    if temp_flag < 1 then
      return temp_region_Id;
    end if;

    select ATTR_NAME, PARENT_ID, ATTR_LEVEL into temp_attr_name, temp_parent_id,temp_attr_level
      from DM_HLR_LINK where ATTR_ID=strAttrId;

    if temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_1 and temp_attr_level <> DM_PKG_CONST.ATTR_LEVEL_2 then
      return temp_region_Id;
    end if;

    if temp_attr_level=DM_PKG_CONST.ATTR_LEVEL_2 then

      select count(*) into temp_flag from DM_HLR_LINK where ATTR_ID=temp_parent_id;
      if temp_flag < 1 then
        return temp_region_Id;
      end if;

      select ATTR_NAME into temp_attr_name from DM_HLR_LINK where ATTR_ID=temp_parent_id;
    end if;

    select count(*) into temp_flag from DM_REGION where REGION_NAME=temp_attr_name;
    if temp_flag < 1 then
        return temp_region_Id;
    end if;

    select REGION_ID into temp_region_Id from DM_REGION where REGION_NAME=temp_attr_name;

    return temp_region_Id;
  end getRegionIdByAttrId;

end DM_PKG_UTILS;
//

delimiter ;//
